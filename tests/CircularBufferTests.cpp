#include <catch2/catch_test_macros.hpp>

#include <data/CircularBuffer.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <latch>
#include <optional>
#include <string>
#include <thread>

TEST_CASE("CircularBuffer is empty after construction", "[CircularBuffer][dequeue]") {
  cudp::data::CircularBuffer<int> buffer(3);

  REQUIRE_FALSE(buffer.dequeue().has_value());
}

TEST_CASE("CircularBuffer with zero capacity rejects every element", "[CircularBuffer][boundary]") {
  cudp::data::CircularBuffer<int> buffer(0);

  REQUIRE_FALSE(buffer.tryEnqueue(42));
  REQUIRE_FALSE(buffer.dequeue().has_value());
}

TEST_CASE("CircularBuffer with unit capacity stores exactly one element", "[CircularBuffer][boundary]") {
  cudp::data::CircularBuffer<int> buffer(1);

  REQUIRE(buffer.tryEnqueue(10));
  REQUIRE_FALSE(buffer.tryEnqueue(20));

  const std::optional<int> element = buffer.dequeue();
  REQUIRE(element.has_value());
  REQUIRE(*element == 10);
  REQUIRE_FALSE(buffer.dequeue().has_value());
}

TEST_CASE("CircularBuffer dequeues elements in FIFO order", "[CircularBuffer][fifo]") {
  cudp::data::CircularBuffer<int> buffer(3);

  REQUIRE(buffer.tryEnqueue(10));
  REQUIRE(buffer.tryEnqueue(20));
  REQUIRE(buffer.tryEnqueue(30));

  REQUIRE(buffer.dequeue() == 10);
  REQUIRE(buffer.dequeue() == 20);
  REQUIRE(buffer.dequeue() == 30);
  REQUIRE_FALSE(buffer.dequeue().has_value());
}

TEST_CASE("CircularBuffer peek preserves the oldest element", "[CircularBuffer][peek]") {
  cudp::data::CircularBuffer<int> buffer(2);

  REQUIRE_FALSE(buffer.peek().has_value());
  REQUIRE(buffer.tryEnqueue(10));
  REQUIRE(buffer.tryEnqueue(20));

  REQUIRE(buffer.peek() == 10);
  REQUIRE(buffer.peek() == 10);
  REQUIRE(buffer.dequeue() == 10);
  REQUIRE(buffer.peek() == 20);
  REQUIRE(buffer.dequeue() == 20);
  REQUIRE_FALSE(buffer.peek().has_value());
}

TEST_CASE("CircularBuffer does not overwrite elements when full", "[CircularBuffer][capacity]") {
  cudp::data::CircularBuffer<int> buffer(2);

  REQUIRE(buffer.tryEnqueue(10));
  REQUIRE(buffer.tryEnqueue(20));
  REQUIRE_FALSE(buffer.tryEnqueue(30));

  REQUIRE(buffer.dequeue() == 10);
  REQUIRE(buffer.dequeue() == 20);
  REQUIRE_FALSE(buffer.dequeue().has_value());
}

TEST_CASE("CircularBuffer reuses storage after wrapping its indices", "[CircularBuffer][wrap-around]") {
  cudp::data::CircularBuffer<int> buffer(3);

  REQUIRE(buffer.tryEnqueue(10));
  REQUIRE(buffer.tryEnqueue(20));
  REQUIRE(buffer.tryEnqueue(30));
  REQUIRE(buffer.dequeue() == 10);

  REQUIRE(buffer.tryEnqueue(40));
  REQUIRE_FALSE(buffer.tryEnqueue(50));
  REQUIRE(buffer.dequeue() == 20);
  REQUIRE(buffer.dequeue() == 30);
  REQUIRE(buffer.dequeue() == 40);
  REQUIRE_FALSE(buffer.dequeue().has_value());

  REQUIRE(buffer.tryEnqueue(50));
  REQUIRE(buffer.tryEnqueue(60));
  REQUIRE(buffer.dequeue() == 50);
  REQUIRE(buffer.dequeue() == 60);
  REQUIRE_FALSE(buffer.dequeue().has_value());
}

TEST_CASE("CircularBuffer stores a copy of the enqueued value", "[CircularBuffer][copy]") {
  cudp::data::CircularBuffer<std::string> buffer(1);
  std::string value = "original";

  REQUIRE(buffer.tryEnqueue(value));
  value = "modified";

  REQUIRE(buffer.dequeue() == "original");
}

TEST_CASE("CircularBuffer serializes concurrent enqueue attempts", "[CircularBuffer][thread-safety]") {
  constexpr std::size_t buffer_capacity = 8;
  constexpr std::size_t contender_count = 32;
  cudp::data::CircularBuffer<std::uint32_t> buffer(static_cast<std::uint32_t>(buffer_capacity));

  std::array<bool, contender_count> enqueue_results{};
  std::array<std::thread, contender_count> contenders;
  std::latch start_signal(1);

  // Try to insert N values (contender_count) with different threads
  for (std::size_t index = 0; index < contender_count; ++index) {
    contenders[index] = std::thread([&buffer, &enqueue_results, &start_signal, index]() {
      start_signal.wait();
      enqueue_results[index] = buffer.tryEnqueue(static_cast<std::uint32_t>(index));
    });
  }

  // Start all the threads
  start_signal.count_down();

  // Wait for them to finish
  for (std::thread &contender : contenders) {
    contender.join();
  }

  // Verify that independently on the number of used threads, the number of accepted enqueues is the buffer capacity
  REQUIRE(std::count(enqueue_results.begin(), enqueue_results.end(), true) == buffer_capacity);

  std::array<bool, contender_count> dequeued_values{};

  for (std::size_t index = 0; index < buffer_capacity; ++index) {
    // Check that every dequeue has an element
    const std::optional<std::uint32_t> element = buffer.dequeue();
    REQUIRE(element.has_value());

    // Check that the element index (a.k.a. the thread that enqueued the data) is
    // compliant with the threads number
    REQUIRE(element.value() < contender_count);

    // Check that the same thread did not do multiple enqueues
    REQUIRE_FALSE(dequeued_values[element.value()]);
    dequeued_values[element.value()] = true;
  }

  // Verify that after all the dequeues, the buffer does not contain any data
  REQUIRE_FALSE(buffer.dequeue().has_value());
}
