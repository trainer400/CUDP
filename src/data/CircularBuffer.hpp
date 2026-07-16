#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

namespace cudp {
namespace data {

/**
 * @brief A thread safe circular buffer with limited number of elements.
 * The dynamic allocation is performed only at init time and the access
 * to the structure is protected by synchronization primitives
 */
template <typename T> class CircularBuffer {
public:
  // Move/Copy constructors deleted due to synchronization primitives
  CircularBuffer(const CircularBuffer &) = delete;
  CircularBuffer(CircularBuffer &&)      = delete;

  /**
   * @brief Creates a circular buffer object that contains N elements as the
   * p_element_size parameter specifies
   */
  explicit CircularBuffer(uint32_t p_element_size);

  /**
   * @brief Get the circular buffer array size
   */
  uint32_t size() {
    std::scoped_lock l(m_buffer_mutex);
    return m_buffer.size() - 1;
  }

  /**
   * @brief Tries to emplace an element T that needs to be copyable.
   * The emplace operation might fail due to limited space inside the buffer
   */
  [[nodiscard]] bool tryEnqueue(const T &p_t);

  /**
   * @brief The method dequeues an element from the circular buffer.
   * If the buffer results to be empty, the optional is returned void
   */
  std::optional<T> dequeue();

  /**
   * @brief Returns a copy of the oldest element without removing it.
   * If the buffer is empty, the optional is returned void
   */
  std::optional<T> peek();

private:
  // Circular buffer memory
  std::vector<T> m_buffer;
  std::mutex m_buffer_mutex;

  // Start/End indices
  uint32_t m_start_index = 0;
  uint32_t m_end_index   = 0;
};

// The usage of a +1 element is the sentinel technique, which allows the buffer
// to distinguish the cases in which the buffer is full/empty. The +1 cell remains
// always not used
template <typename T>
CircularBuffer<T>::CircularBuffer(uint32_t p_element_size)
    : m_buffer(static_cast<typename std::vector<T>::size_type>(p_element_size) + 1U) {}

template <typename T> bool CircularBuffer<T>::tryEnqueue(const T &p_t) {
  std::scoped_lock lock(m_buffer_mutex);

  // The next position of the buffer will be the starting one (hence the buffer is full). A
  // sentinel space is present (and not used) to distinguish the two cases (buffer full and buffer empty).
  // In this case the buffer full implies the usage of the next end_index
  const uint32_t next_end_index = static_cast<uint32_t>((static_cast<typename std::vector<T>::size_type>(m_end_index) + 1U) % m_buffer.size());
  if (next_end_index == m_start_index) {
    return false;
  }

  // Enqueue the data and increase the index
  m_buffer[m_end_index] = p_t;
  m_end_index           = next_end_index;
  return true;
}

template <typename T> std::optional<T> CircularBuffer<T>::dequeue() {
  std::scoped_lock lock(m_buffer_mutex);

  // The buffer is empty
  if (m_start_index == m_end_index) {
    return std::nullopt;
  }

  // Dequeue the element from the buffer and increase the starting index
  std::optional<T> element(m_buffer[m_start_index]);
  m_start_index = static_cast<uint32_t>((static_cast<typename std::vector<T>::size_type>(m_start_index) + 1U) % m_buffer.size());
  return element;
}

template <typename T> std::optional<T> CircularBuffer<T>::peek() {
  std::scoped_lock lock(m_buffer_mutex);

  // The buffer is empty
  if (m_start_index == m_end_index) {
    return std::nullopt;
  }

  return m_buffer[m_start_index];
}

} // namespace data
} // namespace cudp
