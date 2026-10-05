//! Disabled-by-default deterministic crash points used by integration harnesses.

use anyhow::{bail, Result};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum SinkFailpoint {
    #[default]
    Disabled,
    AfterDbCommitBeforeOffsetCommit,
}

impl SinkFailpoint {
    pub const ENV: &'static str = "PULSE_SINK_FAILPOINT";
    pub const NAME: &'static str = "after_db_commit_before_offset_commit";

    pub fn parse(value: Option<&str>) -> Result<Self> {
        match value {
            None | Some("") => Ok(Self::Disabled),
            Some(Self::NAME) => Ok(Self::AfterDbCommitBeforeOffsetCommit),
            Some(other) => bail!("unknown PULSE_SINK_FAILPOINT value {other:?}"),
        }
    }
}
