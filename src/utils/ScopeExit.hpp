#pragma once

#include <utility>

namespace cudp {
namespace utils {

/**
 * @brief The class implements a RAII technique to call the passed function at destruction
 */
template <typename F> class ScopeExit {
public:
  // No copy nor move constructors
  ScopeExit(const ScopeExit &)            = delete;
  ScopeExit &operator=(const ScopeExit &) = delete;

  // Forward the function
  explicit ScopeExit(F &&f)
      : m_func(std::forward<F>(f)) {}

  ~ScopeExit() {
    // Call the function when destroying the object
    m_func();
  }

private:
  // Function to call at destruction
  F m_func;
};

} // namespace utils
} // namespace cudp
