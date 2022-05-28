use std::marker;
use std::ops::{BitOr, Shl, Shr};
use num::FromPrimitive;

pub(crate) trait GribInt<I> {
    fn as_grib_int(&self) -> I;
}

macro_rules! add_impl_for_ints {
    ($(($ty_src:ty, $ty_dst:ty),)*) => ($(
        impl GribInt<$ty_dst> for $ty_src {
            fn as_grib_int(&self) -> $ty_dst {
                if self.leading_zeros() == 0 {
                    let abs = (self << 1 >> 1) as $ty_dst;
                    -abs
                } else {
                    *self as $ty_dst
                }
            }
        }
    )*);
}

add_impl_for_ints! {
    (u8, i8),
    (u16, i16),
    (u32, i32),
    (u64, i64),
}


pub(crate) struct BitwiseIterator<'a, T: 'a + FromPrimitive + Shr<usize, Output = T> + Shl<usize, Output = T> + BitOr<Output = T>> {
    slice: &'a [u8],
    size: usize,
    pos: usize,
    offset: usize,
    _marker: marker::PhantomData<T>,
}

impl<'a, T: 'a + FromPrimitive + Shr<usize, Output = T> + Shl<usize, Output = T> + BitOr<Output = T>> BitwiseIterator<'a, T> {
    pub(crate) fn new(slice: &'a [u8], size: usize) -> Self {
        Self {
            slice,
            size,
            pos: 0,
            offset: 0,
            _marker: Default::default()
        }
    }

    pub(crate) fn with_offset(self, offset: usize) -> Self {
        Self {
            offset,
            ..self
        }
    }
}

impl<'a, T: 'a + FromPrimitive + Shr<usize, Output = T> + Shl<usize, Output = T> + BitOr<Output = T>> Iterator for BitwiseIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {

        let new_offset = self.offset + self.size;
        let (new_pos, new_offset) = (self.pos + new_offset / 8, new_offset % 8);

        if self.pos >= self.slice.len()
            || new_pos > self.slice.len()
            || (new_pos == self.slice.len() && new_offset > 0)
        {
            return None;
        }

        if self.offset >= 8 {
            debug!("{}", self.offset);
        }

        let mut val = T::from_u8(self.slice[self.pos] << self.offset >> self.offset).expect("casted from u8");
        if new_pos == self.pos {
            val = val >> (8 - new_offset); // 00_____# -> 000_____
        } else {
            for pos in (self.pos + 1)..new_pos {
                val = (val << 8) | T::from_u8(self.slice[pos]).expect("casted from u8");
            }

            if new_offset > 0 {
                let last_val = T::from_u8(self.slice[new_pos]).expect("casted from u8") >> (8 - new_offset);
                val = (val << new_offset) | last_val // 0000____ and -####### -> 000____-
            }
        }

        self.pos = new_pos;
        self.offset = new_offset;
        //self.offset += self.size;

        Some(val)
    }
}

pub(crate) struct Buffer {
    pub(crate) bytes: Vec<u8>,
    pos: usize
}

impl Buffer {
    pub(crate) fn new(buf: Vec<u8>) -> Self {
        Self {
            bytes: buf,
            pos: 0
        }
    }

    pub(crate) fn read<T: EndianRead>(&mut self) -> T {
        let end = self.pos + std::mem::size_of::<T>();
        let val = T::from_be_bytes(&self.bytes[self.pos..end]);
        self.pos = end;

        val
    }
}

pub(crate) trait EndianRead {
    fn from_be_bytes(bytes: &[u8]) -> Self;
}

macro_rules! uint_impl {
    ($ty:ty) => {

        impl EndianRead for $ty {
            fn from_be_bytes(bytes: &[u8]) -> Self {
                <$ty>::from_be_bytes(bytes.try_into().unwrap())
            }
        }
    }
}

uint_impl! { u8 }
uint_impl! { u16 }
uint_impl! { u32 }

uint_impl! { f32 }
