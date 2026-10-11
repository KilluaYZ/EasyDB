//! 位图：页内槽位的占用标记（高位在前，与 C++ 版 `bitmap.h` 一致）。
//!
//! 当前 slotted-page 记录层用「记录数只增」的槽目录管理空间，位图保留为
//! 独立组件（单测覆盖，供后续空间回收等功能使用）。

const BITMAP_WIDTH: usize = 8;
const BITMAP_HIGHEST_BIT: u8 = 0x80;

fn bucket(pos: usize) -> usize {
    pos / BITMAP_WIDTH
}

fn bit(pos: usize) -> u8 {
    BITMAP_HIGHEST_BIT >> (pos % BITMAP_WIDTH)
}

/// 整段清零。
pub fn init(bm: &mut [u8]) {
    bm.fill(0);
}

/// `pos` 位置 1。
pub fn set(bm: &mut [u8], pos: usize) {
    bm[bucket(pos)] |= bit(pos);
}

/// `pos` 位清 0。
pub fn reset(bm: &mut [u8], pos: usize) {
    bm[bucket(pos)] &= !bit(pos);
}

/// `pos` 位是否为 1。
pub fn is_set(bm: &[u8], pos: usize) -> bool {
    bm[bucket(pos)] & bit(pos) != 0
}

/// 在 `curr + 1 .. max_n` 范围内找第一个值为 `bit` 的位；找不到返回 `max_n`。
pub fn next_bit(bit_want: bool, bm: &[u8], max_n: usize, curr: i64) -> usize {
    let mut i = (curr + 1).max(0) as usize;
    while i < max_n {
        if is_set(bm, i) == bit_want {
            return i;
        }
        i += 1;
    }
    max_n
}

/// 找第一个值为 `bit` 的位（等价 `next_bit(.., curr = -1)`）。
pub fn first_bit(bit_want: bool, bm: &[u8], max_n: usize) -> usize {
    next_bit(bit_want, bm, max_n, -1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_reset_is_set() {
        let mut bm = [0u8; 4];
        init(&mut bm);
        assert!(!is_set(&bm, 0));
        set(&mut bm, 0);
        set(&mut bm, 7);
        set(&mut bm, 8);
        set(&mut bm, 31);
        assert!(is_set(&bm, 0));
        assert!(is_set(&bm, 7));
        assert!(is_set(&bm, 8));
        assert!(is_set(&bm, 31));
        reset(&mut bm, 7);
        assert!(!is_set(&bm, 7));
        assert!(is_set(&bm, 8)); // 相邻位不受影响
    }

    #[test]
    fn find_bits() {
        let mut bm = [0u8; 2];
        set(&mut bm, 3);
        set(&mut bm, 5);
        assert_eq!(first_bit(true, &bm, 16), 3);
        assert_eq!(first_bit(false, &bm, 16), 0);
        assert_eq!(next_bit(true, &bm, 16, 3), 5);
        assert_eq!(next_bit(true, &bm, 16, 5), 16); // 找不到返回 max_n
        assert_eq!(next_bit(false, &bm, 16, 3), 4);
    }
}
