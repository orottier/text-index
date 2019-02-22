use std::cmp::Ord;
use std::collections::BTreeMap;
use std::fmt::Debug;

pub fn chunk_map<K: Debug + Ord + Clone, V>(
    map: &mut BTreeMap<K, V>,
    pieces: usize,
) -> Vec<(K, BTreeMap<K, V>)> {
    let mut chunked_map: Vec<(K, BTreeMap<K, V>)> = Vec::with_capacity(pieces);

    let count = map.len();
    let chunk_size = count / pieces;

    let mut prev_split = map.keys().nth(0).unwrap().clone();

    for _ in 0..pieces - 1 {
        let split_at = map.keys().nth(chunk_size).unwrap().clone();

        let mut split = map.split_off(&split_at);
        std::mem::swap(&mut split, map);

        chunked_map.push((prev_split, split));
        prev_split = split_at;
    }

    let first = map.keys().nth(0).unwrap().clone();
    let split = map.split_off(&first);
    chunked_map.push((prev_split, split));

    chunked_map
}
