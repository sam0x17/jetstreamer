//! Firehose plugin + CLI that measures the per-block cost the `getInflationReward`
//! JSON-RPC endpoint incurs under partitioned epoch rewards (SIMD-0015), using
//! real mainnet blocks streamed from Old Faithful.
//!
//! Background: `getInflationReward` groups the requested addresses by reward
//! partition and performs one `get_block` per unique partition. Each `get_block`
//! (`blockstore.get_rooted_block`) reads a full block and deserializes +
//! `sanitize()`s every transaction in it. The reward-partition blocks are the
//! first `num_partitions` blocks of an epoch (the RPC reads
//! `get_blocks_with_limit(first_block_of_epoch + 1, num_partitions)`). So a
//! single request's worst case is: read the epoch-boundary block plus the K most
//! expensive partition blocks, where K is the address-count cap.
//!
//! This tool replays that window and measures, per block, the transaction
//! deserialize + `sanitize()` cost and serialized byte volume -- the dominant,
//! hardware-portable component of a `get_block` call. It deliberately does NOT
//! measure the RocksDB read latency itself (that is disk/cache dependent and an
//! attacker largely amortizes it by warming the cache with repeated requests);
//! the byte volume is reported as the I/O proxy.
//!
//! Usage:
//!   cargo run --release --bin inflation_reward_cost -- <epoch> [window_slots]

use {
    clickhouse::Client,
    dashmap::DashMap,
    jetstreamer::{
        JetstreamerRunner,
        firehose::{BlockData, TransactionData, epochs},
        plugin::{Plugin, PluginFuture},
    },
    solana_transaction::versioned::VersionedTransaction,
    std::{
        hint::black_box,
        sync::{Arc, Mutex},
        time::Instant,
    },
};

#[derive(Default, Clone, Copy)]
struct BlockCost {
    tx_count: usize,
    reported_tx_count: u64,
    bytes: usize,
    deser_sanitize_nanos: u128,
}

/// Measures, per reward-partition block, the transaction deserialize + sanitize
/// cost and byte volume that `getInflationReward` forces a node to perform.
#[derive(Default)]
struct InflationRewardCost {
    /// Transactions buffered per slot until the block completes.
    pending: DashMap<u64, Vec<VersionedTransaction>>,
    /// Finalized per-block cost, keyed by slot.
    blocks: DashMap<u64, BlockCost>,
    /// `(slot, num_partitions)` of the epoch-boundary block.
    boundary: Mutex<Option<(u64, u64)>>,
}

impl InflationRewardCost {
    fn measure(txs: &[VersionedTransaction], reported_tx_count: u64) -> BlockCost {
        // Serialize once (setup, not timed) to obtain the wire bytes a blockstore
        // read would deserialize.
        let encoded: Vec<Vec<u8>> = txs
            .iter()
            .map(|tx| bincode::serialize(tx).expect("serialize versioned tx"))
            .collect();
        let bytes = encoded.iter().map(Vec::len).sum();

        // Time deserialize + sanitize -- what `get_rooted_block` does per tx.
        let start = Instant::now();
        for b in &encoded {
            let tx: VersionedTransaction = bincode::deserialize(b).expect("deserialize versioned tx");
            let _ = black_box(tx.sanitize());
            black_box(&tx);
        }
        let deser_sanitize_nanos = start.elapsed().as_nanos();

        BlockCost {
            tx_count: txs.len(),
            reported_tx_count,
            bytes,
            deser_sanitize_nanos,
        }
    }

    fn report(&self) {
        let Some((boundary_slot, num_partitions)) = *self.boundary.lock().unwrap() else {
            eprintln!(
                "ERROR: no epoch-boundary block (num_partitions) seen in the window; \
                 widen --window or check the epoch"
            );
            return;
        };

        // The RPC reads `get_blocks_with_limit(boundary + 1, num_partitions)`:
        // the first `num_partitions` blocks after the boundary, in slot order.
        let partitions: Vec<BlockCost> = {
            let mut v: Vec<(u64, BlockCost)> = self
                .blocks
                .iter()
                .filter(|e| *e.key() > boundary_slot)
                .map(|e| (*e.key(), *e.value()))
                .collect();
            v.sort_by_key(|(slot, _)| *slot);
            v.truncate(num_partitions as usize);
            v.into_iter().map(|(_, c)| c).collect()
        };

        let boundary_cost = self
            .blocks
            .get(&boundary_slot)
            .map(|c| *c)
            .unwrap_or_default();

        if (partitions.len() as u64) < num_partitions {
            eprintln!(
                "WARNING: only saw {} of {num_partitions} partition blocks; widen --window for a \
                 complete picture",
                partitions.len()
            );
        }

        // Sanity: buffered tx counts should match the block's reported count.
        let mismatches = partitions
            .iter()
            .filter(|c| c.tx_count as u64 != c.reported_tx_count)
            .count();
        if mismatches > 0 {
            eprintln!(
                "NOTE: {mismatches}/{} partition blocks had buffered tx count != reported \
                 executed_transaction_count (ordering/vote filtering); numbers are approximate",
                partitions.len()
            );
        }

        let total_ms = partitions.iter().map(|c| c.deser_sanitize_nanos as f64).sum::<f64>() / 1e6;
        let total_mb = partitions.iter().map(|c| c.bytes as f64).sum::<f64>() / 1e6;
        let total_tx: usize = partitions.iter().map(|c| c.tx_count).sum();

        println!("\n# epoch-boundary slot:        {boundary_slot}");
        println!("# reward partitions:          {num_partitions}");
        println!("# partition blocks measured:  {}", partitions.len());
        println!(
            "# boundary block:             {} tx, {:.2} MB, {:.2} ms deser+sanitize",
            boundary_cost.tx_count,
            boundary_cost.bytes as f64 / 1e6,
            boundary_cost.deser_sanitize_nanos as f64 / 1e6,
        );
        println!("# all partitions (no limit):  {total_tx} tx, {total_mb:.1} MB, {total_ms:.1} ms deser+sanitize");

        // Worst case for a cap of K: the attacker picks addresses hitting the K
        // most expensive partition blocks.
        let mut by_cost = partitions.clone();
        by_cost.sort_by_key(|c| std::cmp::Reverse(c.deser_sanitize_nanos));

        println!("\n# worst-case single-request cost vs address-count cap");
        println!("# = boundary block + K most-expensive partition blocks");
        println!("{:>8} {:>12} {:>10} {:>12}", "cap(K)", "blocks_read", "MB_read", "deser_ms");
        let caps = [16usize, 32, 64, 128, 256, 512, num_partitions as usize];
        let mut seen_all = false;
        for &cap in &caps {
            if seen_all {
                break;
            }
            let k = cap.min(partitions.len());
            if cap as u64 >= num_partitions {
                seen_all = true;
            }
            let ms = (boundary_cost.deser_sanitize_nanos as f64
                + by_cost.iter().take(k).map(|c| c.deser_sanitize_nanos as f64).sum::<f64>())
                / 1e6;
            let mb = (boundary_cost.bytes as f64
                + by_cost.iter().take(k).map(|c| c.bytes as f64).sum::<f64>())
                / 1e6;
            let label = if seen_all { format!("{k}*") } else { k.to_string() };
            println!("{label:>8} {:>12} {mb:>10.1} {ms:>12.1}", k + 1);
        }
        println!("# * = no cap (all partitions). blocks_read includes the boundary block.");
        println!(
            "# NOTE: deser_ms is transaction deserialize + sanitize only (the dominant, \
             portable CPU cost of get_rooted_block); it excludes RocksDB read latency."
        );
    }
}

impl Plugin for InflationRewardCost {
    fn name(&self) -> &'static str {
        "inflation-reward-cost"
    }

    fn on_transaction<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        tx: &'a TransactionData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            self.pending
                .entry(tx.slot)
                .or_default()
                .push(tx.transaction.clone());
            Ok(())
        })
    }

    fn on_block<'a>(
        &'a self,
        _thread_id: usize,
        _db: Option<Arc<Client>>,
        block: &'a BlockData,
    ) -> PluginFuture<'a> {
        Box::pin(async move {
            if let BlockData::Block {
                slot,
                rewards,
                executed_transaction_count,
                ..
            } = block
            {
                let txs = self
                    .pending
                    .remove(slot)
                    .map(|(_, v)| v)
                    .unwrap_or_default();
                let cost = Self::measure(&txs, *executed_transaction_count);
                self.blocks.insert(*slot, cost);
                if let Some(n) = rewards.num_partitions {
                    *self.boundary.lock().unwrap() = Some((*slot, n));
                }
            }
            Ok(())
        })
    }

    fn on_exit(&self, _db: Option<Arc<Client>>) -> PluginFuture<'_> {
        Box::pin(async move {
            self.report();
            Ok(())
        })
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <epoch> [window_slots]", args[0]);
        std::process::exit(2);
    }
    let epoch: u64 = args[1].parse().expect("epoch must be a u64");
    let window: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(800);

    // This tool needs no ClickHouse.
    unsafe {
        std::env::set_var("JETSTREAMER_CLICKHOUSE_MODE", "off");
    }

    let (start, _end_inclusive) = epochs::epoch_to_slot_range(epoch);
    eprintln!(
        "measuring epoch {epoch}: slots [{start}, {}) (sequential replay from Old Faithful)",
        start + window
    );

    JetstreamerRunner::new()
        .with_log_level("warn")
        .with_sequential(true)
        .with_plugin(Box::new(InflationRewardCost::default()))
        .with_slot_range_bounds(start, start + window)
        .run()
        .expect("jetstreamer runner failed");
}
