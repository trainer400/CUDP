#include <network/CUDP.hpp>

namespace cudp {
namespace network {

std::optional<std::shared_ptr<CUDP>> CUDP::create(std::unique_ptr<Transceiver> p_transceiver,
                                                  std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback) {
  // Check input validity before creating the object
  if (!p_transceiver || !p_new_connection_callback)
    return std::nullopt;

  return std::shared_ptr<CUDP>(new CUDP(std::move(p_transceiver), p_new_connection_callback));
}

CUDP::CUDP(std::unique_ptr<Transceiver> p_transceiver, std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback)
    : m_socket(std::move(p_transceiver))
    , m_new_connection_callback(p_new_connection_callback) {}

} // namespace network
} // namespace cudp