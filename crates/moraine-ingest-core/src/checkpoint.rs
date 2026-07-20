use crate::model::Checkpoint;
use std::collections::HashMap;

pub(crate) fn checkpoint_key(source_name: &str, source_file: &str) -> String {
    // Length-prefixed fields preserve boundaries even when either field contains newlines.
    format!(
        "{}:{}{}:{}",
        source_name.len(),
        source_name,
        source_file.len(),
        source_file
    )
}

pub(crate) fn merge_checkpoint(pending: &mut HashMap<String, Checkpoint>, checkpoint: Checkpoint) {
    let key = checkpoint_key(&checkpoint.source_name, &checkpoint.source_file);
    match pending.get(&key) {
        None => {
            pending.insert(key, checkpoint);
        }
        Some(existing) => {
            // Causal revisions make rewinds and equal-offset lifecycle
            // transitions ordered. Fall back to the legacy generation/offset
            // rule only while loading or writing a pre-publication schema.
            let replace = if checkpoint.checkpoint_revision > 0 || existing.checkpoint_revision > 0
            {
                checkpoint.checkpoint_revision > existing.checkpoint_revision
                    || (checkpoint.checkpoint_revision == existing.checkpoint_revision
                        && checkpoint.operation_id == existing.operation_id)
            } else {
                checkpoint.source_generation > existing.source_generation
                    || (checkpoint.source_generation == existing.source_generation
                        && checkpoint.last_offset >= existing.last_offset)
            };
            if replace {
                pending.insert(key, checkpoint);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{checkpoint_key, merge_checkpoint};
    use crate::model::Checkpoint;
    use std::collections::HashMap;

    fn checkpoint(
        source_name: &str,
        source_file: &str,
        generation: u32,
        last_offset: u64,
    ) -> Checkpoint {
        Checkpoint {
            source_name: source_name.to_string(),
            source_file: source_file.to_string(),
            source_generation: generation,
            last_offset,
            status: "active".to_string(),
            ..Checkpoint::default()
        }
    }

    #[test]
    fn checkpoint_key_disambiguates_newline_inputs() {
        let left = checkpoint_key("alpha\nbeta", "gamma");
        let right = checkpoint_key("alpha", "beta\ngamma");

        assert_ne!(left, right);
    }

    #[test]
    fn merge_checkpoint_keeps_distinct_newline_variants() {
        let mut pending = HashMap::<String, Checkpoint>::new();
        let first = checkpoint("alpha\nbeta", "gamma", 1, 10);
        let second = checkpoint("alpha", "beta\ngamma", 1, 20);

        let first_key = checkpoint_key(&first.source_name, &first.source_file);
        let second_key = checkpoint_key(&second.source_name, &second.source_file);

        merge_checkpoint(&mut pending, first);
        merge_checkpoint(&mut pending, second);

        assert_eq!(pending.len(), 2);
        assert!(pending.contains_key(&first_key));
        assert!(pending.contains_key(&second_key));
    }

    #[test]
    fn causal_revision_allows_same_generation_rewind() {
        let mut pending = HashMap::<String, Checkpoint>::new();
        let mut active = checkpoint("kiro", "/tmp/state", 3, 100);
        active.checkpoint_revision = 10;
        active.operation_id = "active-10".to_string();
        let mut rewind = checkpoint("kiro", "/tmp/state", 3, 40);
        rewind.status = "replaying".to_string();
        rewind.checkpoint_revision = 11;
        rewind.operation_id = "rewind-11".to_string();

        merge_checkpoint(&mut pending, active);
        merge_checkpoint(&mut pending, rewind);

        let current = pending.values().next().expect("checkpoint");
        assert_eq!(current.last_offset, 40);
        assert_eq!(current.status, "replaying");
        assert_eq!(current.checkpoint_revision, 11);
    }

    #[test]
    fn stale_causal_transition_cannot_regress_state() {
        let mut pending = HashMap::<String, Checkpoint>::new();
        let mut newer = checkpoint("cursor", "/tmp/db", 2, 20);
        newer.checkpoint_revision = 9;
        newer.operation_id = "newer".to_string();
        let mut stale = checkpoint("cursor", "/tmp/db", 5, 1_000);
        stale.checkpoint_revision = 8;
        stale.operation_id = "stale".to_string();

        merge_checkpoint(&mut pending, newer);
        merge_checkpoint(&mut pending, stale);

        assert_eq!(pending.values().next().unwrap().checkpoint_revision, 9);
    }
}
