use anyhow::{bail, Context, Result};

use crate::owner::{ClickHouseError, ClickHouseErrorCategory};

pub const SAFETY_BUDGET_BYTES: u64 = 1 << 30;
pub const MAX_MODULUS: u64 = 1 << 14;

pub fn is_memory_limit_exceeded(error: &anyhow::Error) -> bool {
    for cause in error.chain() {
        if let Some(ch) = cause.downcast_ref::<ClickHouseError>() {
            if ch.exception_code() == Some(241) {
                return true;
            }
            if ch.category() == ClickHouseErrorCategory::ResourceExhausted {
                if let Some(detail) = ch.exception_detail() {
                    if detail.contains("MEMORY_LIMIT") {
                        return true;
                    }
                }
            }
        }
    }
    false
}

pub fn can_split(modulus: u64, max_modulus: u64) -> bool {
    modulus <= max_modulus / 2
}

pub fn push_split_children(stack: &mut Vec<(u64, u64)>, modulus: u64, shard: u64) {
    stack.push((modulus * 2, shard * 2 + 1));
    stack.push((modulus * 2, shard * 2));
}

pub fn run_adaptive_hash_shards(
    mut execute: impl FnMut(u64, u64) -> Result<()>,
    is_oom: impl Fn(&anyhow::Error) -> bool,
    max_modulus: u64,
) -> Result<()> {
    let mut stack = vec![(1u64, 0u64)];
    while let Some((modulus, shard)) = stack.pop() {
        if modulus > max_modulus {
            bail!(
                "adaptive hash insert exceeded max modulus {max_modulus}; \
                 a single partition still exceeds the safety budget"
            );
        }
        match execute(modulus, shard) {
            Ok(()) => {}
            Err(err) if is_oom(&err) => {
                if !can_split(modulus, max_modulus) {
                    return Err(err).context(format!(
                        "adaptive hash insert OOM at modulus {modulus} shard {shard}"
                    ));
                }
                push_split_children(&mut stack, modulus, shard);
            }
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub fn render_hash_insert_sql(template: &str, modulus: u64, shard: u64, budget: u64) -> String {
    template
        .replace("{modulus}", &modulus.to_string())
        .replace("{shard}", &shard.to_string())
        .replace("{budget}", &budget.to_string())
}

pub fn join_lookup_memory_budget(map_bytes: u64) -> u64 {
    let estimated = map_bytes.saturating_mul(4);
    estimated.max(SAFETY_BUDGET_BYTES)
}

pub fn parse_byte_estimate(text: &str) -> Result<u64> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(0);
    }
    text.parse::<u64>()
        .with_context(|| format!("invalid byte estimate: {text:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::bail;
    use std::cell::RefCell;

    #[test]
    fn small_corpus_runs_one_shard() {
        let calls = RefCell::new(Vec::new());
        run_adaptive_hash_shards(
            |modulus, shard| {
                calls.borrow_mut().push((modulus, shard));
                Ok(())
            },
            |_| false,
            MAX_MODULUS,
        )
        .unwrap();
        assert_eq!(*calls.borrow(), vec![(1, 0)]);
    }

    #[test]
    fn oom_at_root_splits_once() {
        let calls = RefCell::new(Vec::new());
        run_adaptive_hash_shards(
            |modulus, shard| {
                calls.borrow_mut().push((modulus, shard));
                if modulus == 1 {
                    bail!("oom");
                }
                Ok(())
            },
            |err| err.to_string().contains("oom"),
            MAX_MODULUS,
        )
        .unwrap();
        assert_eq!(*calls.borrow(), vec![(1, 0), (2, 0), (2, 1)]);
    }

    #[test]
    fn deep_oom_splits_only_failing_branch() {
        let calls = RefCell::new(Vec::new());
        run_adaptive_hash_shards(
            |modulus, shard| {
                calls.borrow_mut().push((modulus, shard));
                if modulus < 4 || (modulus == 4 && shard == 3) {
                    bail!("oom");
                }
                Ok(())
            },
            |err| err.to_string().contains("oom"),
            MAX_MODULUS,
        )
        .unwrap();
        assert_eq!(
            *calls.borrow(),
            vec![
                (1, 0),
                (2, 0),
                (4, 0),
                (4, 1),
                (2, 1),
                (4, 2),
                (4, 3),
                (8, 6),
                (8, 7),
            ]
        );
    }

    #[test]
    fn persistent_oom_hits_max_modulus() {
        let err = run_adaptive_hash_shards(
            |_, _| bail!("oom"),
            |err| err.to_string().contains("oom"),
            4,
        )
        .unwrap_err();
        assert!(err.to_string().contains("max modulus") || err.to_string().contains("OOM at"));
    }

    #[test]
    fn non_oom_errors_do_not_split() {
        let calls = RefCell::new(Vec::new());
        let err = run_adaptive_hash_shards(
            |modulus, shard| {
                calls.borrow_mut().push((modulus, shard));
                bail!("syntax");
            },
            |err| err.to_string().contains("oom"),
            MAX_MODULUS,
        )
        .unwrap_err();
        assert!(err.to_string().contains("syntax"));
        assert_eq!(*calls.borrow(), vec![(1, 0)]);
    }

    #[test]
    fn render_substitutes_placeholders() {
        let sql = render_hash_insert_sql(
            "WHERE x % {modulus} = {shard} SETTINGS max_memory_usage = {budget}",
            8,
            3,
            1024,
        );
        assert_eq!(sql, "WHERE x % 8 = 3 SETTINGS max_memory_usage = 1024");
    }

    #[test]
    fn join_budget_scales_with_map_and_floors_at_safety() {
        assert_eq!(join_lookup_memory_budget(0), SAFETY_BUDGET_BYTES);
        assert_eq!(join_lookup_memory_budget(100), SAFETY_BUDGET_BYTES);
        assert_eq!(
            join_lookup_memory_budget(SAFETY_BUDGET_BYTES),
            SAFETY_BUDGET_BYTES * 4
        );
    }

    #[test]
    fn parse_byte_estimate_accepts_empty_as_zero() {
        assert_eq!(parse_byte_estimate("").unwrap(), 0);
        assert_eq!(parse_byte_estimate("  \n").unwrap(), 0);
        assert_eq!(parse_byte_estimate("42").unwrap(), 42);
        assert!(parse_byte_estimate("nope").is_err());
    }
}
