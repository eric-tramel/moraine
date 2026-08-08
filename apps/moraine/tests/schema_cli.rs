use std::process::Command;

#[test]
fn analytics_schema_matches_the_exact_public_byte_contract() {
    let output = Command::new(env!("CARGO_BIN_EXE_moraine"))
        .args(["schema", "analytics"])
        .output()
        .expect("run moraine schema analytics");

    let expected = include_bytes!("../src/commands/analytics_schema_v1.golden.json");

    assert!(output.status.success(), "status: {}", output.status);
    assert_eq!(output.stdout, expected);
    assert!(
        output.stderr.is_empty(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
