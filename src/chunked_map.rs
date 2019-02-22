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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_one() {
        let mut map = BTreeMap::new();
        map.insert(1, 'a');
        map.insert(2, 'b');
        map.insert(3, 'c');

        let chunked_map = chunk_map(&mut map, 1);
        assert_eq!(chunked_map.len(), 1);

        let (key, chunk) = &chunked_map[0];
        assert_eq!(key, &1);
        assert_eq!(chunk.len(), 3);
        assert_eq!(chunk.get(&1), Some(&'a'));
        assert_eq!(chunk.get(&2), Some(&'b'));
        assert_eq!(chunk.get(&3), Some(&'c'));
    }

    #[test]
    fn test_split_two() {
        let mut map = BTreeMap::new();
        map.insert(1, 'a');
        map.insert(2, 'b');
        map.insert(3, 'c');

        let chunked_map = chunk_map(&mut map, 2);
        assert_eq!(chunked_map.len(), 2);

        let (key, chunk) = &chunked_map[0];
        assert_eq!(key, &1);
        assert_eq!(chunk.len(), 1);
        assert_eq!(chunk.get(&1), Some(&'a'));

        let (key, chunk) = &chunked_map[1];
        assert_eq!(key, &2);
        assert_eq!(chunk.len(), 2);
        assert_eq!(chunk.get(&2), Some(&'b'));
        assert_eq!(chunk.get(&3), Some(&'c'));
    }
}
