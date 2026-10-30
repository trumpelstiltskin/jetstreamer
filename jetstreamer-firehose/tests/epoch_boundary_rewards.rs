use futures_util::FutureExt;
use jetstreamer_firehose::firehose::{
    BlockData, OnEntryFn, OnErrorFn, OnRewardFn, OnStatsTrackingFn, TransactionData, firehose,
};
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[tokio::test]
#[ignore = "requires access to the Old Faithful mainnet archive"]
async fn vat_debit_boundary_keeps_blocks_and_transactions() {
    let blocks = Arc::new(Mutex::new(BTreeSet::new()));
    let transactions = Arc::new(AtomicU64::new(0));
    let run = firehose(
        1,
        false,
        false,
        None,
        460_512_000..460_512_003,
        Some({
            let blocks = blocks.clone();
            move |_: usize, block: BlockData| {
                let blocks = blocks.clone();
                async move {
                    if let BlockData::Block { slot, .. } = block {
                        blocks.lock().unwrap().insert(slot);
                    }
                    Ok(())
                }
                .boxed()
            }
        }),
        Some({
            let transactions = transactions.clone();
            move |_: usize, _: TransactionData| {
                transactions.fetch_add(1, Ordering::Relaxed);
                async { Ok(()) }.boxed()
            }
        }),
        None::<OnEntryFn>,
        None::<OnRewardFn>,
        None::<OnErrorFn>,
        None::<OnStatsTrackingFn>,
        None,
    );
    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("boundary replay timed out")
        .expect("boundary replay failed");
    assert_eq!(
        *blocks.lock().unwrap(),
        BTreeSet::from([460_512_000, 460_512_001, 460_512_002])
    );
    assert!(transactions.load(Ordering::Relaxed) > 0);
}
