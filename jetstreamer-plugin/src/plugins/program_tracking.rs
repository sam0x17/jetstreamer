use std::{
    cell::RefCell,
    collections::HashMap,
    fs::File,
    io::Write,
    sync::{Arc, RwLock},
    sync::atomic::{AtomicBool, Ordering},
};

use clickhouse::Client;
use futures_util::FutureExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use solana_sdk::{message::VersionedMessage, pubkey::Pubkey};
use solana_sdk::{message::VersionedMessage, pubkey::Pubkey};

use crate::{
    Plugin, PluginFuture,
    bridge::{Block, Transaction},
};

thread_local! {
    static ACCOUNT_SLOTS: RefCell<HashMap<Pubkey, u64>> = RefCell::new(HashMap::new());
}

// Global storage for collecting data from all threads
static GLOBAL_ACCOUNT_SLOTS: Lazy<Arc<RwLock<HashMap<Pubkey, u64>>>> = 
    Lazy::new(|| Arc::new(RwLock::new(HashMap::new())));

static SHOULD_SAVE_ON_EXIT: AtomicBool = AtomicBool::new(true);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AccountSlotEntry {
    pub account: Pubkey,
    pub highest_slot: u64,
}

#[derive(Debug, Default, Clone)]
pub struct AccountSlotTrackingPlugin;

impl AccountSlotTrackingPlugin {
    /// Flushes thread-local data to global storage
    pub fn flush_thread_local_data() {
        Self::extract_local_data()
            .and_then(Self::update_global_storage)
            .map(|_| Self::clear_local_storage())
            .unwrap_or_else(|e| log::debug!("Flush skipped: {}", e));
    }
    
    fn extract_local_data() -> Result<Vec<(Pubkey, u64)>, &'static str> {
        ACCOUNT_SLOTS.with(|slots| {
            let slots = slots.borrow();
            if slots.is_empty() {
                Err("No local data to flush")
            } else {
                Ok(slots.iter().map(|(&account, &slot)| (account, slot)).collect())
            }
        })
    }
    
    fn update_global_storage(data: Vec<(Pubkey, u64)>) -> Result<(), &'static str> {
        let mut global_slots = GLOBAL_ACCOUNT_SLOTS.write()
            .map_err(|_| "Failed to acquire write lock")?;
            
        for (account, slot) in data {
            global_slots.entry(account)
                .and_modify(|current| *current = (*current).max(slot))
                .or_insert(slot);
        }
        Ok(())
    }
    
    fn clear_local_storage() {
        ACCOUNT_SLOTS.with(|slots| slots.borrow_mut().clear());
    }

    /// Collects all account slot data from all threads and returns it sorted by slot (descending)
    pub fn collect_and_sort_data() -> Vec<AccountSlotEntry> {
        // First, flush any remaining thread-local data
        Self::flush_thread_local_data();
        
        let mut entries = Vec::new();
        
        if let Ok(global_slots) = GLOBAL_ACCOUNT_SLOTS.read() {
            for (&account, &highest_slot) in global_slots.iter() {
                entries.push(AccountSlotEntry {
                    account,
                    highest_slot,
                });
            }
        }

        // Sort by highest_slot in descending order
        entries.sort_by(|a, b| b.highest_slot.cmp(&a.highest_slot));
        entries
    }
    
    /// Saves the account slot data to disk in the specified format
    pub fn save_to_disk(filename: &str) -> Result<(), Box<dyn std::error::Error>> {
        let data = Self::collect_and_sort_data();
        let mut file = File::create(filename)?;
        
        // Write as JSON for readability, could also use bincode for efficiency
        let json_data = serde_json::to_string_pretty(&data)?;
        file.write_all(json_data.as_bytes())?;
        
        log::info!("Saved {} account slot entries to {}", data.len(), filename);
        Ok(())
    }
    
    /// Periodically flush thread-local data to global storage
    /// Call this occasionally to prevent excessive memory usage in thread-local storage
    pub fn periodic_flush() {
        ACCOUNT_SLOTS.with(|local_slots| {
            let local_len = local_slots.borrow().len();
            // Flush when we have accumulated a significant amount of data
            if local_len > 10000 {
                Self::flush_thread_local_data();
            }
        });
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
            
            // Update the highest slot for each account in this transaction
            ACCOUNT_SLOTS.with(|slots| {
                let mut slots = slots.borrow_mut();
                for &account in account_keys {
                    slots.entry(account)
                        .and_modify(|current_slot| {
                            if slot > *current_slot {
                                *current_slot = slot;
                            }
                        })
                        .or_insert(slot);
                }
            });

            // Periodically flush to prevent excessive memory usage
            Self::periodic_flush();

            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_block(&self, _: Arc<Client>, _: Block) -> PluginFuture<'_> {
        async move { Ok(()) }.boxed()
    }

    #[inline(always)]
    fn on_load(&self, _db: Arc<Client>) -> PluginFuture<'_> {
        async move {
            log::info!("Account Slot Tracking Plugin loaded.");
            Ok(())
        }
        .boxed()
    }

    #[inline(always)]
    fn on_exit(&self, _db: Arc<Client>) -> PluginFuture<'_> {
        async move {
            log::info!("Account Slot Tracking Plugin unloading...");
            
            // Ensure all thread-local data is flushed before saving
            Self::flush_thread_local_data();
            
            if SHOULD_SAVE_ON_EXIT.load(Ordering::Relaxed) {
                if let Err(e) = Self::save_to_disk("account_slots.json") {
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
