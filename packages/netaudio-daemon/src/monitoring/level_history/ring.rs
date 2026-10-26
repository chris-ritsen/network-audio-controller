use std::ops::{Index, IndexMut};

const PAGE_SLOTS: usize = 32;

pub(super) struct Ring<T, const LENGTH: usize> {
    pages: Box<[Option<Box<[T]>>]>,
    empty: T,
}

impl<T: Copy + Default, const LENGTH: usize> Default for Ring<T, LENGTH> {
    fn default() -> Self {
        Self {
            pages: vec![None; LENGTH.div_ceil(PAGE_SLOTS)].into_boxed_slice(),
            empty: T::default(),
        }
    }
}

impl<T, const LENGTH: usize> Index<usize> for Ring<T, LENGTH> {
    type Output = T;

    fn index(&self, slot: usize) -> &Self::Output {
        assert!(slot < LENGTH);
        self.pages[slot / PAGE_SLOTS]
            .as_ref()
            .map_or(&self.empty, |page| &page[slot % PAGE_SLOTS])
    }
}

impl<T: Copy, const LENGTH: usize> IndexMut<usize> for Ring<T, LENGTH> {
    fn index_mut(&mut self, slot: usize) -> &mut Self::Output {
        assert!(slot < LENGTH);
        let index = slot / PAGE_SLOTS;
        let length = PAGE_SLOTS.min(LENGTH - index * PAGE_SLOTS);
        let empty = self.empty;
        let page = self.pages[index].get_or_insert_with(|| vec![empty; length].into_boxed_slice());
        &mut page[slot % PAGE_SLOTS]
    }
}
