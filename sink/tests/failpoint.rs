use pulse_sink::failpoint::SinkFailpoint;

#[test]
fn failpoint_is_disabled_for_missing_or_empty_values() {
    assert_eq!(SinkFailpoint::parse(None).unwrap(), SinkFailpoint::Disabled);
    assert_eq!(
        SinkFailpoint::parse(Some("")).unwrap(),
        SinkFailpoint::Disabled
    );
}

#[test]
fn failpoint_accepts_only_the_database_to_offset_crash_point() {
    assert_eq!(
        SinkFailpoint::parse(Some("after_db_commit_before_offset_commit")).unwrap(),
        SinkFailpoint::AfterDbCommitBeforeOffsetCommit
    );
}

#[test]
fn unknown_nonempty_failpoint_is_a_startup_error() {
    assert!(SinkFailpoint::parse(Some("after_db_commit")).is_err());
    assert!(SinkFailpoint::parse(Some(" after_db_commit_before_offset_commit")).is_err());
}
