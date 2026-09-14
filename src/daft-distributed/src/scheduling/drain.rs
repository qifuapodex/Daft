//! Local retirement barrier. Data holds are query-scoped, established before dispatch
//! and released only after quiescence and acknowledged shuffle cleanup.
use std::collections::HashSet;

use common_error::{DaftError, DaftResult};
use serde::Serialize;

use crate::plan::QueryIdx;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum DrainState {
    #[default]
    Active,
    Draining,
    ReadyToRetire,
    Retiring,
    Retired,
    Unknown,
    Failed,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct DrainBarrier {
    pub epoch: u64,
    pub state: DrainState,
    pub data_queries: HashSet<QueryIdx>,
    pub unknown: Option<String>,
}

impl DrainBarrier {
    pub fn can_retire(&self, active_tasks: usize) -> bool {
        active_tasks == 0 && self.data_queries.is_empty() && self.unknown.is_none()
    }

    pub fn status(&self, active_tasks: usize) -> DrainState {
        if self.unknown.is_some() {
            DrainState::Unknown
        } else if self.state == DrainState::Draining && self.can_retire(active_tasks) {
            DrainState::ReadyToRetire
        } else {
            self.state
        }
    }

    pub fn prepare(&mut self, epoch: u64) -> DaftResult<()> {
        if epoch < self.epoch || epoch == 0 {
            return Err(DaftError::ValueError("Stale drain epoch".into()));
        }
        if epoch == self.epoch {
            return if self.state == DrainState::Active {
                Err(DaftError::ValueError("Drain epoch was cancelled".into()))
            } else {
                Ok(())
            };
        }
        if matches!(self.state, DrainState::Retiring | DrainState::Retired) {
            return Err(DaftError::ValueError(
                "Worker retirement is committed".into(),
            ));
        }
        self.epoch = epoch;
        self.state = DrainState::Draining;
        Ok(())
    }

    pub fn cancel(&mut self, epoch: u64) -> DaftResult<()> {
        self.check_epoch(epoch)?;
        if matches!(self.state, DrainState::Retiring | DrainState::Retired) {
            return Err(DaftError::ValueError(
                "Worker retirement is committed".into(),
            ));
        }
        self.state = DrainState::Active;
        Ok(())
    }

    pub fn check_epoch(&self, epoch: u64) -> DaftResult<()> {
        if epoch == 0 || epoch != self.epoch {
            Err(DaftError::ValueError("Stale drain epoch".into()))
        } else {
            Ok(())
        }
    }

    pub fn commit(&mut self, epoch: u64, active_tasks: usize) -> DaftResult<()> {
        self.check_epoch(epoch)?;
        if self.status(active_tasks) != DrainState::ReadyToRetire {
            return Err(DaftError::ValueError(
                "Worker is not ready to retire".into(),
            ));
        }
        self.state = DrainState::Retiring;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_holds_survive_idle_and_other_query_cleanup() {
        let mut barrier = DrainBarrier::default();
        // Hold precedes output publication and task completion, and is shared
        // by all consumers/builders/reconstruction attempts until query cleanup.
        barrier.data_queries.extend([1, 2]);
        barrier.prepare(1).unwrap();
        assert!(!barrier.can_retire(0));
        barrier.data_queries.remove(&1);
        assert!(!barrier.can_retire(0));
        barrier.data_queries.remove(&2);
        assert!(!barrier.can_retire(1));
        assert_eq!(barrier.status(0), DrainState::ReadyToRetire);
    }

    #[test]
    fn epochs_and_unknown_are_fail_closed() {
        let mut barrier = DrainBarrier::default();
        barrier.prepare(4).unwrap();
        barrier.cancel(4).unwrap();
        assert!(barrier.prepare(4).is_err());
        barrier.prepare(5).unwrap();
        assert!(barrier.cancel(4).is_err());
        barrier.unknown = Some("cancel acknowledgement missing".into());
        assert!(barrier.commit(5, 0).is_err());
        barrier.unknown = None;
        barrier.commit(5, 0).unwrap();
        assert!(barrier.cancel(5).is_err());
        barrier.state = DrainState::Retired;
        assert!(barrier.prepare(6).is_err());
    }
}
