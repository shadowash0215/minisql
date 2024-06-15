#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (IsPageFree(next_free_page_)) {
    // Allocate the page
    bytes[next_free_page_ / 8] |= (1 << (next_free_page_ % 8));
    page_allocated_++;
    page_offset = next_free_page_;
    // Find the next free page
    for (uint32_t i = next_free_page_ + 1; i < GetMaxSupportedSize(); i++) {
      if (IsPageFree(i)) {
        next_free_page_ = i;
        break;
      }
    }
    return true;
  }
  return false;
}

/**
 * Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }
  if (IsPageFree(page_offset)) {
    return false;
  }
  // Deallocate the page
  bytes[page_offset / 8] &= ~(1 << (page_offset % 8));
  page_allocated_--;
  // Update next free page
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }
  return true;
}

/**
 * Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }
  return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if (byte_index >= MAX_CHARS) {
    return false;
  }
  if (bit_index >= 8) {
    return false;
  }
  return (bytes[byte_index] & (1 << bit_index)) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;