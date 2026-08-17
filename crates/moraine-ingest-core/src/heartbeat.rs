/// Human-readable diagnostic label only. Publication and shared-checkpoint
/// authority comes from the durable `PublicationIdentity`, never this value.
pub(crate) fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_else(|| "localhost".to_string())
}
