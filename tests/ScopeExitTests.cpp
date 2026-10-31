#include <catch2/catch_test_macros.hpp>

#include <utils/ScopeExit.hpp>

#include <stdexcept>

TEST_CASE("ScopeExit calls its callback exactly once on scope exit", "[ScopeExit][destruction]") {
  int call_count = 0;

  {
    cudp::utils::ScopeExit scope_exit([&call_count]() { ++call_count; });

    REQUIRE(call_count == 0);
  }

  REQUIRE(call_count == 1);
}

TEST_CASE("ScopeExit calls its callback when an exception leaves the scope", "[ScopeExit][exception]") {
  int call_count = 0;

  REQUIRE_THROWS_AS(
      [&call_count]() {
        cudp::utils::ScopeExit scope_exit([&call_count]() { ++call_count; });

        throw std::runtime_error("scope failure");
      }(),
      std::runtime_error);

  REQUIRE(call_count == 1);
}
