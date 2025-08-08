use std::{
    fs::File,
    io::Write,
    sync::{
        Arc,
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

// High-performance concurrent HashMap using internal sharding
// Multiple threads can write to different shards simultaneously!
static ACCOUNT_SLOTS: Lazy<DashMap<Pubkey, u64>> = Lazy::new(|| DashMap::new());

static SHOULD_SAVE_ON_EXIT: AtomicBool = AtomicBool::new(true);

// Store the end slot for detecting when processing is complete
static END_SLOT: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountSlotEntry {
    pub account: Pubkey,
    pub highest_slot: u64,
}

#[derive(Debug, Default, Clone)]
pub struct AccountSlotTrackingPlugin;

impl AccountSlotTrackingPlugin {
    /// Parse slot range from command line arguments (same logic as main app)
    fn parse_slot_range_from_args() -> Option<std::ops::Range<u64>> {
        let first_arg = std::env::args().nth(1)?;

        if first_arg.contains(':') {
            let (slot_a, slot_b) = first_arg.split_once(':')?;
            let slot_a: u64 = slot_a.parse().ok()?;
            let slot_b: u64 = slot_b.parse().ok()?;
            Some(slot_a..(slot_b + 1))
        } else {
            let _epoch: u64 = first_arg.parse().ok()?;
            // Note: This would require importing geyser_replay::epochs
            // For now, we'll just return None for epoch-based ranges
            log::warn!("Epoch-based slot ranges not supported for end-slot detection");
            None
        }
    }

    /// Initialize the end slot from command line arguments
    fn initialize_end_slot() {
        if let Some(range) = Self::parse_slot_range_from_args() {
            let end_slot = range.end - 1; // Convert from exclusive to inclusive end
            END_SLOT.store(end_slot, Ordering::SeqCst);
            log::info!("🎯 Will save data when reaching end slot: {}", end_slot);
        } else {
            log::warn!("Could not determine end slot from command line arguments");
        }
    }

    /// Check if we've reached the end slot and save data if so
    fn check_and_save_if_complete(slot: u64) {
        let mut end_slot = END_SLOT.load(Ordering::SeqCst);
        if end_slot == 0 {
            log::info!("No end slot, initializing end slot from args range");
            Self::initialize_end_slot();
            end_slot = END_SLOT.load(Ordering::SeqCst);
        }

        if (end_slot > 0 && slot >= end_slot) || (slot % 100000 == 0) {
            log::info!("🏁 Reached end slot {}, saving final data...", end_slot);
            if let Err(e) = Self::save_to_disk(&format!("account_slots_{slot}.bin")) {
                log::error!("❌ Failed to save final data: {}", e);
            } else {
                log::info!("✅ Successfully saved final account slot data!");
            }
        }
    }

    /// Collects all account slot data and returns it sorted by slot (descending)
    pub fn collect_and_sort_data() -> Vec<AccountSlotEntry> {
        // DashMap provides a simple iterator over all key-value pairs
        let entries: Vec<AccountSlotEntry> = ACCOUNT_SLOTS
            .iter()
            .map(|entry| AccountSlotEntry {
                account: *entry.key(),
                highest_slot: *entry.value(),
            })
            .collect();

        // Sort by highest_slot in descending order
        let mut sorted_entries = entries;
        sorted_entries.sort_by(|a, b| b.highest_slot.cmp(&a.highest_slot));
        sorted_entries
    }

    /// Saves the account slot data to disk in the specified format
    pub fn save_to_disk(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        let data = Self::collect_and_sort_data();

        // Get absolute path for debugging
        let absolute_path = std::env::current_dir()?.join(filename);
        let mut file = File::create(&absolute_path)?;

        // Use bincode for efficient binary serialization
        let binary_data = bincode::serialize(&data)?;
        file.write_all(&binary_data)?;

        log::info!(
            "✅ Saved {} account slot entries to {} ({} bytes)",
            data.len(),
            absolute_path.display(),
            binary_data.len()
        );
        eprintln!(
            "✅ Saved {} account slot entries to {} ({} bytes)",
            data.len(),
            absolute_path.display(),
            binary_data.len()
        );
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
            let account_keys = match transaction.tx.message {
                VersionedMessage::Legacy(ref msg) => &msg.account_keys,
                VersionedMessage::V0(ref msg) => &msg.account_keys,
            };

            let slot = transaction.slot;
            log::debug!(
                "📦 Processing transaction at slot {} with {} accounts",
                slot,
                account_keys.len()
            );

            // Update the highest slot for each account in this transaction
            // DashMap allows concurrent updates to different keys without blocking!
            for &account in account_keys {
                ACCOUNT_SLOTS
                    .entry(account)
                    .and_modify(|current_slot| {
                        if slot > *current_slot {
                            *current_slot = slot;
                        }
                    })
                    .or_insert(slot);
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block(&self, _: Arc<Client>, block: Block) -> PluginFuture<'_> {
        async move {
            // Check if we've reached the end slot and save data if so
            Self::check_and_save_if_complete(block.slot);
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_load(&self, _db: Arc<Client>) -> PluginFuture<'_> {
        async move {
            log::info!("🔧 Account Slot Tracking Plugin loaded and ready!");

            // Initialize the end slot from command line args
            Self::initialize_end_slot();

            // Test save immediately to verify it works
            if let Err(e) = Self::save_to_disk("account_slots_initial.bin") {
                log::warn!("Failed to create initial save file: {}", e);
            }

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, _db: Arc<Client>) -> PluginFuture<'_> {
        async move {
            log::info!("Account Slot Tracking Plugin unloading...");

            if SHOULD_SAVE_ON_EXIT.load(Ordering::Relaxed) {
                if let Err(e) = Self::save_to_disk("account_slots.bin") {
                    log::error!("Failed to save account slot data: {}", e);
                } else {
                    log::info!("Account slot data saved successfully.");
                }
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
