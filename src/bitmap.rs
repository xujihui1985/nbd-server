#[derive(Clone, Debug)]
pub struct Bitmap {
    len: usize,
    bytes: Vec<u8>,
}

impl Bitmap {
    pub fn new(len: usize) -> Self {
        let byte_len = len.div_ceil(8);
        Self {
            len,
            bytes: vec![0; byte_len],
        }
    }

    pub fn from_bytes(len: usize, bytes: Vec<u8>) -> Self {
        Self { len, bytes }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, index: usize) -> bool {
        let byte = index / 8;
        let bit = index % 8;
        self.bytes
            .get(byte)
            .map(|value| value & (1 << bit) != 0)
            .unwrap_or(false)
    }

    pub fn set(&mut self, index: usize, value: bool) {
        let byte = index / 8;
        let bit = index % 8;
        if let Some(slot) = self.bytes.get_mut(byte) {
            if value {
                *slot |= 1 << bit;
            } else {
                *slot &= !(1 << bit);
            }
        }
    }

    pub fn clear_all(&mut self) {
        self.bytes.fill(0);
    }

    pub fn count_ones(&self) -> usize {
        self.bytes
            .iter()
            .map(|value| value.count_ones() as usize)
            .sum()
    }

    pub fn iter_set_bits(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len).filter(|index| self.get(*index))
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::Bitmap;

    #[test]
    fn bitmap_round_trip() {
        let mut bitmap = Bitmap::new(10);
        bitmap.set(1, true);
        bitmap.set(9, true);

        assert!(bitmap.get(1));
        assert!(bitmap.get(9));
        assert_eq!(bitmap.count_ones(), 2);

        let restored = Bitmap::from_bytes(10, bitmap.bytes().to_vec());
        assert!(restored.get(1));
        assert!(restored.get(9));
    }
}
