use std::{
    fs::File,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use bincode;
use clickhouse::Client;
use dashmap::DashMap;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_sdk::{message::VersionedMessage, pubkey::Pubkey};

use crate::{
    Plugin, PluginFuture,
    bridge::{Block, Transaction},
};

/// Convert slot number to epoch number (each epoch has 432,000 slots)
#[inline(always)]
const fn slot_to_epoch(slot: u64) -> u16 {
    (slot / 432_000) as u16
}

// High-performance concurrent HashMap storing detailed account activity
static ACCOUNT_ACTIVITY: Lazy<DashMap<Pubkey, AccountActivity>> = 
    Lazy::new(|| DashMap::new());

// Checkpointing configuration
const DEFAULT_CHECKPOINT_INTERVAL: u64 = 50_000; // Save every 50k slots
const CHECKPOINT_FILENAME: &str = "account_activity_checkpoint.bin";

// Checkpointing state
static CHECKPOINT_INTERVAL: AtomicU64 = AtomicU64::new(DEFAULT_CHECKPOINT_INTERVAL);
static SAVE_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
static LAST_CHECKPOINT_SLOT: AtomicU64 = AtomicU64::new(0);

/// Tracks the top 10 epochs for reads and writes, plus total counts
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AccountActivity {
    /// Top 10 highest epochs where this account was read from (sorted descending)
    pub top_read_epochs: Vec<u16>,
    /// Top 10 highest epochs where this account was written to (sorted descending) 
    pub top_write_epochs: Vec<u16>,
    /// Total number of read operations
    pub read_count: u32,
    /// Total number of write operations
    pub write_count: u32,
}

impl AccountActivity {
    /// Add a read epoch, maintaining top 10 sorted list
    pub fn add_read_epoch(&mut self, epoch: u16) {
        self.read_count += 1;
        Self::add_epoch_to_list(&mut self.top_read_epochs, epoch);
    }
    
    /// Add a write epoch, maintaining top 10 sorted list
    pub fn add_write_epoch(&mut self, epoch: u16) {
        self.write_count += 1;
        Self::add_epoch_to_list(&mut self.top_write_epochs, epoch);
    }
    
    /// Helper to maintain a sorted top-10 list of epochs
    fn add_epoch_to_list(list: &mut Vec<u16>, epoch: u16) {
        // If epoch already exists, don't add duplicate
        if list.contains(&epoch) {
            return;
        }
        
        // Add epoch and keep sorted (descending)
        list.push(epoch);
        list.sort_by(|a, b| b.cmp(a));
        
        // Keep only top 10
        if list.len() > 10 {
            list.truncate(10);
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountActivityEntry {
    pub account: Pubkey,
    pub activity: AccountActivity,
}

#[derive(Debug, Default, Clone)]
pub struct AccountSlotTrackingPlugin;

impl AccountSlotTrackingPlugin {
    /// Extract writable and readonly accounts from message header and account keys
    /// Works for both Legacy and V0 messages since they have the same structure
    fn extract_account_access(
        header: &solana_sdk::message::MessageHeader,
        account_keys: &[solana_sdk::pubkey::Pubkey],
    ) -> (Vec<solana_sdk::pubkey::Pubkey>, Vec<solana_sdk::pubkey::Pubkey>) {
        let num_required_signatures = header.num_required_signatures as usize;
        let num_readonly_signed_accounts = header.num_readonly_signed_accounts as usize;
        let num_readonly_unsigned_accounts = header.num_readonly_unsigned_accounts as usize;
        
        let writable_signed_end = num_required_signatures - num_readonly_signed_accounts;
        let readonly_signed_end = num_required_signatures;
        let writable_unsigned_end = account_keys.len() - num_readonly_unsigned_accounts;
        
        let mut writable = Vec::new();
        let mut readonly = Vec::new();
        
        // Writable signed accounts
        writable.extend_from_slice(&account_keys[0..writable_signed_end]);
        // Read-only signed accounts
        readonly.extend_from_slice(&account_keys[writable_signed_end..readonly_signed_end]);
        // Writable unsigned accounts
        writable.extend_from_slice(&account_keys[readonly_signed_end..writable_unsigned_end]);
        // Read-only unsigned accounts
        readonly.extend_from_slice(&account_keys[writable_unsigned_end..]);
        
        (writable, readonly)
    }

    /// Check if we should create a checkpoint and do so if needed
    /// Thread-safe: prevents multiple threads from writing the same checkpoint
    fn check_and_checkpoint(current_slot: u64) {
        let interval = CHECKPOINT_INTERVAL.load(Ordering::SeqCst);

        // Use divisibility check for checkpoint intervals
        if current_slot % interval == 0 {
            // Atomic check-and-set to prevent duplicate checkpoints from multiple threads
            let last_checkpoint = LAST_CHECKPOINT_SLOT.load(Ordering::SeqCst);
            
            // Only proceed if this slot hasn't been checkpointed yet
            if current_slot > last_checkpoint {
                // Try to claim this checkpoint slot atomically
                if LAST_CHECKPOINT_SLOT.compare_exchange(
                    last_checkpoint, 
                    current_slot, 
                    Ordering::SeqCst, 
                    Ordering::SeqCst
                ).is_ok() {
                    log::info!("💾 Creating checkpoint at slot {} (interval: {})", current_slot, interval);
                    if let Err(e) = Self::save_checkpoint(current_slot) {
                        log::error!("❌ Failed to create checkpoint: {}", e);
                    } else {
                        log::info!("✅ Checkpoint saved successfully");
                    }
                }
            }
        }
    }

    /// Collects all account activity data
    pub fn collect_data() -> Vec<AccountActivityEntry> {
        // DashMap provides a simple iterator over all key-value pairs
        ACCOUNT_ACTIVITY
            .iter()
            .map(|entry| AccountActivityEntry {
                account: *entry.key(),
                activity: entry.value().clone(),
            })
            .collect()
    }
    
    /// Save checkpoint to disk with atomic write to prevent corruption
    fn save_checkpoint(current_slot: u64) -> Result<(), Box<dyn std::error::Error>> {
        let _guard = SAVE_MUTEX.lock().map_err(|e| {
            format!("Failed to acquire save mutex: {}", e)
        })?;
    
        let entries = Self::collect_data();
        let binary_data = bincode::serialize(&entries)?;
        
        // Atomic write: write to temp file first, then rename
        let temp_filename = format!("{}.tmp", CHECKPOINT_FILENAME);
        let mut file = File::create(&temp_filename)?;
        file.write_all(&binary_data)?;
        file.sync_all()?; // Ensure data is written to disk
        drop(file); // Close file before rename
        
        // Atomic rename (on most filesystems)
        std::fs::rename(&temp_filename, CHECKPOINT_FILENAME)?;
        
        log::debug!("💾 Saved checkpoint: {} entries, slot {}, {} bytes", 
                   entries.len(), current_slot, binary_data.len());
        Ok(())
    }

    /// Save final results to a separate output file with atomic write
    pub fn save_final_results(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        let _guard = SAVE_MUTEX.lock().map_err(|e| {
            format!("Failed to acquire save mutex: {}", e)
        })?;

        let data = Self::collect_data();
        let absolute_path = std::env::current_dir()?.join(filename);
        let temp_path = std::env::current_dir()?.join(format!("{}.tmp", filename));
        
        let binary_data = bincode::serialize(&data)?;
        let mut file = File::create(&temp_path)?;
        file.write_all(&binary_data)?;
        file.sync_all()?; // Ensure data is written to disk
        drop(file); // Close file before rename
        
        // Atomic rename
        std::fs::rename(&temp_path, &absolute_path)?;

        log::info!("✅ Saved final results: {} entries to {} ({} bytes)", 
                   data.len(), absolute_path.display(), binary_data.len());
        eprintln!("✅ Saved final results: {} entries to {} ({} bytes)", 
                  data.len(), absolute_path.display(), binary_data.len());
        Ok(())
    }
}

impl Plugin for AccountSlotTrackingPlugin {
    #[inline(always)]
    fn name(&self) -> &'static str {
        "Account Slot Tracking"
    }

    #[inline(always)]
    fn on_transaction(
        &self,
        _db: Arc<Client>,
        transaction: Transaction,
        _tx_index: u32,
    ) -> PluginFuture<'_> {
        async move {
            let slot = transaction.slot;
            let epoch = slot_to_epoch(slot);
            
            // Extract account access information from the message
            // Both Legacy and V0 messages have the same header structure and account_keys layout
            let (writable_accounts, readonly_accounts) = match &transaction.tx.message {
                VersionedMessage::Legacy(msg) => {
                    Self::extract_account_access(&msg.header, &msg.account_keys)
                },
                VersionedMessage::V0(msg) => {
                    Self::extract_account_access(&msg.header, &msg.account_keys)
                }
            };

            log::debug!("📦 Processing transaction at slot {} (epoch {}): {} writable, {} readonly accounts", 
                       slot, epoch, writable_accounts.len(), readonly_accounts.len());

            // Update account activity tracking
            // Process writable accounts (write access)
            for &account in &writable_accounts {
                ACCOUNT_ACTIVITY
                    .entry(account)
                    .and_modify(|activity| activity.add_write_epoch(epoch))
                    .or_insert_with(|| {
                        let mut activity = AccountActivity::default();
                        activity.add_write_epoch(epoch);
                        activity
                    });
            }
            
            // Process readonly accounts (read access)
            for &account in &readonly_accounts {
                ACCOUNT_ACTIVITY
                    .entry(account)
                    .and_modify(|activity| activity.add_read_epoch(epoch))
                    .or_insert_with(|| {
                        let mut activity = AccountActivity::default();
                        activity.add_read_epoch(epoch);
                        activity
                    });
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block(&self, _: Arc<Client>, block: Block) -> PluginFuture<'_> {
        async move {
            // Check if we should create a checkpoint
            //Self::check_and_checkpoint(block.slot);
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, _db: Arc<Client>) -> PluginFuture<'_> {
        async move {
            log::info!("🔧 Account Slot Tracking Plugin loaded and ready!");

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, _db: Arc<Client>) -> PluginFuture<'_> {
        async move {
            log::info!("🏁 Account Slot Tracking Plugin unloading...");

            // Save final results to output file
            if let Err(e) = Self::save_final_results("account_activity_final.bin") {
                log::error!("❌ Failed to save final results: {}", e);
            } else {
                log::info!("✅ Final results saved successfully");
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn clone_plugin(&self) -> Box<dyn Plugin> {
        Box::new(self.clone())
    }
}
