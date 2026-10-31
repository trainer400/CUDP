#include <network/CUDP.hpp>

#include <utils/ScopeExit.hpp>
namespace cudp {
namespace network {

std::optional<std::shared_ptr<CUDP>> CUDP::create(std::shared_ptr<Transceiver> p_transceiver,
                                                  std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback) {
  // Check input validity before creating the object
  if (!p_transceiver || !p_new_connection_callback)
    return std::nullopt;

  // IMPORTANT: Create a shared_ptr specifically using the new operator to avoid weak pointer holding
  // its ownership on destruction. This leverages the difference between make_shared and shared_ptr(new ...)
  // TODO: test that with a catch test
  std::shared_ptr<CUDP> result = std::shared_ptr<CUDP>(new CUDP(p_transceiver, p_new_connection_callback));
  result->startReceive();

  return result;
}

CUDP::CUDP(std::shared_ptr<Transceiver> p_transceiver, std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback)
    : m_socket(p_transceiver)
    , m_new_connection_callback(p_new_connection_callback) {
  // Allocate the receive packet memory
  m_receive_packet = std::make_unique<UDPPacket>();
}

CUDP::ConnectionState::ConnectionState(std::shared_ptr<ConnectionHandler> p_handler)
    : m_handler(p_handler) {
  // Allocate send buffer
  for (uint32_t i = 0; i < CONNECTION_BUFFER_COUNT; i++) {
    m_send_buffers.at(i) = std::make_unique<UDPPacket>();
  }
}

CUDP::~CUDP() { close(); }

bool CUDP::registerConnection(const asio::ip::udp::endpoint &p_endpoint, std::shared_ptr<ConnectionHandler> p_handler) {
  // Socket is closed
  if (m_closed)
    return false;

  // Handler is null
  if (!p_handler)
    return false;

  // Under mutex add the new connection
  std::scoped_lock l(m_state_mutex);
  std::shared_ptr<ConnectionState> new_connection_state = std::make_shared<ConnectionState>(p_handler);
  m_connections.emplace(std::make_pair(p_endpoint, new_connection_state));

  // Initialize the connection handler
  p_handler->init(p_endpoint, weak_from_this());

  return true;
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

  return true;
}

bool CUDP::close() {
  // Set the closed flag to false directly
  m_closed = true;

  // Close the socket if not already closed
  return m_socket->close();
}

void CUDP::startReceive() {
  std::scoped_lock l(m_receive_mutex);

  // If the receiving buffer packet is not owned return immediately (probably a receive operation is already pending)
  if (!m_receive_packet && m_closed)
    return;

  // Get the weak object pointer to allow the CUDP entity to be destroyed even in presence of a non completed asyncReceive
  auto weak_self = weak_from_this();

  // Start the async receive operation with the transceiver. Give ownership of the buffer
  // to the object and expect the callback to be called at some point
  m_socket->asyncReceive(m_receive_packet, [weak_self](std::unique_ptr<UDPPacket> p_packet, uint32_t p_size, asio::ip::udp::endpoint p_source,
                                                       asio::error_code p_error) {
    std::shared_ptr<CUDP> self = weak_self.lock();
    if (self)
      self->handleReceive(std::move(p_packet), p_size, p_source, p_error);
  });
}

void CUDP::handleReceive(std::unique_ptr<UDPPacket> p_packet, uint32_t p_size, asio::ip::udp::endpoint p_source, asio::error_code p_error) {
  // Create the scope exit to call send the next message and startReceive every time the function terminates
  auto scope_exit = utils::ScopeExit([&]() {
    // Reclaim the packet ownership
    m_receive_packet = std::move(p_packet);

    // Send the next packet and start receiving again
    trySendNext();
    startReceive();
  });

  // Check that the receive operation went well
  if (p_error)
    return;

  // Get the corresponding connection if existing
  std::optional<std::shared_ptr<ConnectionState>> connection;
  {
    std::scoped_lock l(m_state_mutex);
    auto connection_it = m_connections.find(p_source);
    connection         = connection_it != m_connections.end() ? connection_it->second : connection;
  }

  // Call the new connection handler if not already present
  if (!connection.has_value()) {
    m_new_connection_callback(p_source);

    // Get the corresponding connection if added by the callback
    std::scoped_lock l(m_state_mutex);
    auto connection_it = m_connections.find(p_source);
    connection         = connection_it != m_connections.end() ? connection_it->second : connection;

    // No connection added
    if (!connection.has_value())
      return;
  }

  // The connection is present, call the connection handler on message callback
  connection.value()->m_handler->onMessage(p_packet->m_packet_data.data(), p_packet->m_data_size);
}

} // namespace network
} // namespace cudp