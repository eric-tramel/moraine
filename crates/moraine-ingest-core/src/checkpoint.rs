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
            let replace = checkpoint.source_generation > existing.source_generation
                || (checkpoint.source_generation == existing.source_generation
                    && checkpoint.last_offset >= existing.last_offset);
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
}
