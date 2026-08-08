use anyhow::{Context, Result};

use crate::owner::{ClickHouseError, ClickHouseErrorCategory};

pub(super) const SAFETY_BUDGET_BYTES: u64 = 1 << 30;
pub(super) const MAX_MODULUS: u64 = 1 << 14;

pub(super) fn is_memory_limit_exceeded(error: &anyhow::Error) -> bool {
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

pub(super) fn can_split(modulus: u64, max_modulus: u64) -> bool {
    modulus <= max_modulus / 2
}

pub(super) fn push_split_children(stack: &mut Vec<(u64, u64)>, modulus: u64, shard: u64) {
    stack.push((modulus * 2, shard + modulus));
    stack.push((modulus * 2, shard));
}

pub(super) fn render_hash_insert_sql(
    template: &str,
    modulus: u64,
    shard: u64,
    budget: u64,
) -> String {
    template
        .replace("{modulus}", &modulus.to_string())
        .replace("{shard}", &shard.to_string())
        .replace("{budget}", &budget.to_string())
}

pub(super) fn join_lookup_memory_budget(map_bytes: u64) -> u64 {
    let estimated = map_bytes.saturating_mul(4);
    estimated.max(SAFETY_BUDGET_BYTES)
}

pub(super) fn parse_byte_estimate(text: &str) -> Result<u64> {
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

    #[test]
    fn split_children_exactly_partition_parent_remainder() {
        let mut children = Vec::new();
        push_split_children(&mut children, 8, 3);

        assert_eq!(children, vec![(16, 11), (16, 3)]);
        let mut child_remainders = children.iter().map(|(_, shard)| *shard).collect::<Vec<_>>();
        child_remainders.sort_unstable();
        let parent_remainders = (0..16)
            .filter(|remainder| remainder % 8 == 3)
            .collect::<Vec<_>>();
        assert_eq!(child_remainders, parent_remainders);
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
