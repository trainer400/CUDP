#include <network/CUDP.hpp>

namespace cudp {
namespace network {

std::optional<std::shared_ptr<CUDP>> CUDP::create(std::shared_ptr<Transceiver> p_transceiver,
                                                  std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback) {
  // Check input validity before creating the object
  if (!p_transceiver || !p_new_connection_callback)
    return std::nullopt;

  return std::make_shared<CUDP>(p_transceiver, p_new_connection_callback);
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

bool CUDP::send(const asio::ip::udp::endpoint &p_destination, const uint8_t *p_data, uint32_t p_size) {
  if (p_data == nullptr || p_size == 0 || p_size >= UDPPacket::MAX_UDP_PKT_SIZE)
    return false;

  // Gather the connection
  std::shared_ptr<ConnectionState> connection;

  {
    // Find the corresponding iterator
    std::scoped_lock l(m_state_mutex);
    const auto &it = m_connections.find(p_destination);

    // Connection not found
    if (it == m_connections.end())
      return false;

    connection = it->second;
  }

  {
    std::scoped_lock l(connection->m_mutex);

    // Try to enqueue inside the circular buffer the send buffer index inside the array.
    // That could fail since the queue may be full.
    bool enqueue_result = connection->m_pending_packets.tryEnqueue(connection->m_next_send_buffer);

    // Buffer is full
    if (!enqueue_result)
      return false;

    // The buffer is not completely full, copy the new data inside the buffer slot
    std::unique_ptr<UDPPacket> &buffer = connection->m_send_buffers.at(connection->m_next_send_buffer);
    std::memcpy(buffer->m_packet_data.data(), p_data, p_size);
    buffer->m_data_size = p_size;

    // Update the next index
    connection->m_next_send_buffer = (connection->m_next_send_buffer + 1) % connection->m_send_buffers.size();
  }
}

} // namespace network
} // namespace cudp