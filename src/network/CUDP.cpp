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

CUDP::~CUDP() {
  m_socket->close();
  m_closed = true;
}

bool CUDP::registerConnection(const asio::ip::udp::endpoint &p_endpoint, std::shared_ptr<ConnectionHandler> p_handler) {
  // Socket is closed
  if (m_closed)
    return false;

  // Handler is null
  if (!p_handler)
    return false;

  // Lock the state mutex to fetch the set
  std::scoped_lock l(m_state_mutex);
  if (m_seen_endpoints.contains(p_endpoint))
    return false;

  // Under mutex add the new connection
  std::shared_ptr<ConnectionState> new_connection_state = std::shared_ptr<ConnectionState>(new ConnectionState(p_handler));
  m_connections.emplace(std::make_pair(p_endpoint, new_connection_state));
  m_seen_endpoints.emplace(p_endpoint);
}

bool CUDP::unregisterConnection(const asio::ip::udp::endpoint &p_endpoint) {
  std::scoped_lock l(m_state_mutex);
  return m_connections.erase(p_endpoint) > 0;
}

} // namespace network
} // namespace cudp