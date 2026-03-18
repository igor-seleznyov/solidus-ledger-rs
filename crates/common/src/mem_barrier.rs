use std::ptr;
use std::sync::atomic::{fence, Ordering};

#[inline(always)]
pub unsafe fn release_store_u8(ptr: *mut u8, val: u8) {
    unsafe {
        fence(Ordering::Release);
        ptr::write_volatile(ptr, val);
    }
}

#[inline(always)]
pub unsafe fn acquire_load_u8(ptr: *const u8) -> u8 {
    let value = unsafe { ptr::read_volatile(ptr) };
    fence(Ordering::Acquire);
    value
}

#[inline(always)]
pub unsafe fn release_store_u64(ptr: *mut u64, val: u64) {
    unsafe {
        fence(Ordering::Release);
        ptr::write_volatile(ptr, val);
    }
}

#[inline(always)]
pub unsafe fn acquire_load_u64(ptr: *const u64) -> u64 {
    let value = unsafe { ptr::read_volatile(ptr) };
    fence(Ordering::Acquire);
    value
}
