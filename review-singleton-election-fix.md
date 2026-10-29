# Review: removed-leader response hang and the singleton election fix

This note reviews two pull requests against `v0.10.0-alpha.37`:

- [PR #1](https://github.com/hughe/openraft/pull/1) adds the regression test
  `tests/tests/membership/t31_remove_leader_delayed_ack.rs`.
- [PR #2](https://github.com/hughe/openraft/pull/2) adds one condition to
  `RaftCore::handle_tick_election` in `openraft/src/core/raft_core.rs`.

## Executive summary

Both PRs are correct: PR #1's test reproduces the hang deterministically on
alpha.37 and passes only with PR #2, and PR #2's one-conjunct guard is a sound,
minimal fix whose stated reasoning holds up under code reading and experiment.
It is not the only sound fix: upstream `main` (commit `60028ef9`, after
alpha.37) already closed the same race incidentally with a different guard, so
PR #2 is a backport that becomes redundant once Lisa moves past that commit.
PR #2's guard is the better of the two for availability, because the successor
takes over about 10 ms after the removal commits versus about 160 ms under
upstream's guard, so keep PR #2 for the alpha.37 line and consider upstreaming
PR #1's test with PR #2's condition OR-ed into upstream's.

## The bug

On alpha.37, `handle_tick_election` lets a node elect itself on the next tick
when it is the only voter in its *effective* membership. That path skips the
election timeout. It also skips Pre-Vote, so the leader lease held by the
current leader does not stop it.

During `change_membership([1], false)` on a `{0,1}` cluster:

1. Node 0 commits the joint configuration `{0,1}` / `{1}`, then appends the
   final configuration `{1}` and replicates it.
2. Node 1 stores `{1}` as its effective membership. Its committed membership is
   still the joint one.
3. On the next tick, node 1 sees itself as the sole effective voter and elects
   itself in a higher term, while node 0 is still alive and leading.
4. Node 1 commits `{1}` and its blank entry with its own vote alone.
5. A heartbeat tells node 0 about the higher vote. Node 0 steps down, which
   drops the replication progress that would have committed `{1}` locally.
6. Node 1 no longer replicates to node 0, because node 0 is not in `{1}`.
   Node 0 never learns that `{1}` is committed and never applies it.

Step-down fails pending linearizable reads but keeps pending write
responders. The membership responder on node 0 therefore waits forever. The
caller sees neither success nor an error.

## PR #1: the regression test

The test is correct and deterministic.

- The post-hook runs after node 1 has handled the append. It delays only the
  reply to the append that carries the final `{1}` entry. The initial `{0,1}`
  entry and the joint entry cannot match, because the matcher requires a
  single-config membership whose only voter is node 1.
- Heartbeats run on a separate worker. Node 0 therefore keeps contacting node 1
  during the 250 ms delay, which is the situation that triggers the shortcut.
- `Config::default()` leaves tick, heartbeat and election enabled. The test
  needs all three, so this is a deliberate exception to the usual advice to
  disable them.
- The expected index is `log_index + 2`: one joint entry and one final entry.
  Node 0 must apply exactly that index. Node 1 may apply more, because it
  appends its own blank entry as the new leader.

Minor points:

- The body does not use the scoped `{ ... }` phase blocks that `CLAUDE.md`
  asks for.
- The doc comment names Lisa and specific alpha versions. Remove these before
  upstreaming.

## PR #2: the fix

PR #2 adds one condition to the shortcut:

```rust
let is_single_campaigner = effective.is_voter(&self.id)
    && effective.voter_ids().count() == 1
    && self.engine.state.membership_state.committed().log_id() == effective.log_id();
```

The PR's reasoning was checked point by point.

- **Log-id comparison.** `MembershipState::ensure_committed` uses the same
  comparison, and the snapshot path documents that equal log ids imply equal
  memberships. The comparison is correct and idiomatic.
- **Single-node bootstrap.** `handle_initialize` calls `engine.elect()`
  directly and never reaches the tick shortcut. Bootstrap latency is unchanged.
- **Fast takeover still works.** When node 0 commits `{1}`, it replicates the
  new commit index to node 1 before it steps down. Node 1 then has
  committed equal to effective and elects on its next tick.
- **Liveness when the old leader dies.** If node 0 crashes before committing,
  node 1 falls back to the normal path and elects after one election timeout.
  Its pre-vote and vote need only its own grant.
- **One behaviour change.** A single-node cluster that restarts with an
  uncommitted latest membership entry now waits one election timeout before
  electing. An example is a crash while adding a learner. Upstream's guard
  keeps that case immediate.

The commit subject uses the `membership:` scope. This repository's history
uses `core:` for changes to `raft_core.rs`.

## Is PR #2 the minimal fix?

Yes. It adds one conjunct and changes no other path. The other candidate fixes
are larger or weaker:

- **Remove the shortcut.** Every single-node restart would wait an election
  timeout.
- **Fail pending write responders on step-down.** The caller would get an
  error instead of a hang. PR #1 requires success, and the change really did
  commit, so an error would misreport the outcome.

## Is PR #2 the best fix?

Upstream `main` already guards the same shortcut differently:

```rust
let is_sole_voter = effective.voter_ids().eq([self.id.clone()]);
let votes_for_self = vote.leader_node_id() == &self.id;
let may_campaign = effective.is_campaigner(&self.id);
let is_single_campaigner = may_campaign && is_sole_voter && votes_for_self;
```

Commit `60028ef9` added `votes_for_self` for configurable memberships, not for
this bug. A follower holding node 0's vote no longer takes the shortcut, so
upstream is also protected. Upstream has no regression test for this case.

The two guards differ in how fast node 1 takes over after a leader removal:

- **PR #2.** Node 1 elects on the first tick after it learns the commit.
- **Upstream.** Node 1 still holds node 0's vote, so it waits one full
  election timeout. That is about 160 ms at test defaults and 1.5 to 3 s at
  Lisa's production timings.

A union of the two conditions keeps both properties. Node 1 would elect
immediately when it holds its own vote, or when its sole-voter membership is
committed:

```rust
let is_single_campaigner = may_campaign
    && is_sole_voter
    && (votes_for_self || committed.log_id() == effective.log_id());
```

This variant was not built or tested.

## Residual risk

This was found by reading the code and was not tested. A removed leader never
fails its pending write responders. Suppose node 0 accepts a client write after
appending `{1}` and before it steps down. If node 0 never learns that the write
committed, that caller can hang in the same way. PR #2 removes the trigger
exercised by PR #1, but not this class of hang.

## Evidence

Each experiment ran in a throwaway worktree.

| Check | Result |
| --- | --- |
| Regression on PR #1 head `87cc4ee0` | Fails with `Elapsed(())` after 2.07 s |
| Regression on PR #2 head `7b15fa21` | Passes 10 of 10 runs in 0.34 s |
| `membership`, `elect`, `life_cycle` suites on PR #2 head | 62, 20 and 14 passed |
| Single-node bootstrap to leader, guard on and off | 46 ms in both cases |
| Successor becomes leader after the response, PR #2 | 5 to 12 ms |
| PR #1 test ported to `main` | Passes 3 of 3 runs |
| Same test on `main` with `votes_for_self` removed | Fails with `Elapsed(())` |
| Successor becomes leader after the response, `main` | 157 to 165 ms |

Porting the test to `main` needed two adjustments. `main` has a different
router and post-hook API. The removal index on `main` is not `log_index + 2`,
so the port read the index from the response instead.

To run the regression on this branch:

```bash
cargo test -p tests --test membership remove_leader_with_delayed_ack
```
