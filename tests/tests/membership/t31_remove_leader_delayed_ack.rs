use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::RPCTypes;
use openraft::async_runtime::WatchReceiver;
use openraft::entry::RaftEntry;
use openraft::type_config::TypeConfigExt;
use openraft_memstore::TypeConfig;

use crate::fixtures::RaftRouter;
use crate::fixtures::rpc_request::RpcRequest;
use crate::fixtures::ut_harness;

/// Characterize alpha.37's removal race: the new singleton elects before the old
/// leader receives its final membership acknowledgement, leaving the caller pending.
/// The stacked fix changes this oracle to require successful completion.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn remove_leader_with_delayed_ack() -> Result<()> {
    let config = Arc::new(Config::default().validate()?);
    let mut router = RaftRouter::new(config);
    let log_index = router.new_cluster(btreeset! {0, 1}, btreeset! {}).await?;
    let leader = router.get_raft_handle(&0)?;
    let successor = router.get_raft_handle(&1)?;

    tracing::info!(
        log_index,
        "--- delay the final membership acknowledgement past the next heartbeat"
    );
    router
        .set_rpc_post_hook(RPCTypes::AppendEntries, |_router, req, _resp, from, to| {
            let delay = match req {
                RpcRequest::AppendEntries(req) => {
                    from == 0
                        && to == 1
                        && req.entries.iter().any(|entry| {
                            entry.get_membership().is_some_and(|m| {
                                m.get_joint_config().len() == 1 && m.voter_ids().collect::<Vec<_>>() == vec![1]
                            })
                        })
                }
                _ => false,
            };
            Box::pin(async move {
                if delay {
                    TypeConfig::sleep(Duration::from_millis(250)).await;
                }
                Ok(())
            })
        })
        .await;

    tracing::info!(log_index, "--- remove node 0 without retaining it as a learner");
    let result = TypeConfig::timeout(Duration::from_secs(2), leader.change_membership([1], false)).await;
    let old_metrics = leader.metrics().borrow_watched().clone();
    let new_metrics = successor.metrics().borrow_watched().clone();
    tracing::info!(?old_metrics, ?new_metrics, ?result, "--- inspect removal outcome");
    leader.shutdown().await?;
    successor.shutdown().await?;
    // This assertion deliberately describes the bug, keeping the reproduction
    // branch green while verifying that it is this race, not a setup failure.
    assert!(
        result.is_err(),
        "expected the removed leader's response to remain pending"
    );
    let removal_index = log_index + 2;
    assert_eq!(old_metrics.last_log_index, Some(removal_index));
    assert_eq!(old_metrics.last_applied.map(|id| id.index), Some(removal_index - 1));
    assert_eq!(new_metrics.current_leader, Some(1));
    assert!(new_metrics.last_applied.map(|id| id.index) >= Some(removal_index));
    assert_eq!(
        new_metrics.committed_membership_config.membership().get_joint_config(),
        &vec![btreeset! {1}]
    );
    Ok(())
}
