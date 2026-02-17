use crate::model::Checkpoint;
use std::collections::HashMap;

pub(crate) fn checkpoint_key(source_name: &str, source_file: &str) -> String {
    format!("{}\n{}", source_name, source_file)
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
