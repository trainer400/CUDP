#include <network/UDPSocket.hpp>

namespace cudp {
namespace network {

UDPSocket::UDPSocket(asio::io_context &p_io_context, asio::ip::udp::endpoint p_endpoint)
    : m_io_context(p_io_context)
    , m_endpoint(p_endpoint)
    , m_socket(m_io_context, m_endpoint) {}

void UDPSocket::setOnSendCallback(std::function<void(std::unique_ptr<UDPPacket>)> p_callback) {
  std::scoped_lock<std::mutex> l(m_send_callback_mutex);
  m_send_callback = std::move(p_callback);
}

void UDPSocket::setOnReceiveCallback(std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::error_code)> p_callback) {
  std::scoped_lock<std::mutex> l(m_receive_callback_mutex);
  m_receive_callback = std::move(p_callback);
}

bool UDPSocket::asyncSend(asio::ip::udp::endpoint p_destination, std::unique_ptr<UDPPacket> &p_tx_buffer, uint32_t p_size) {
  // Invalid input is rejected before borrowing the packet, so the caller keeps ownership
  if (p_tx_buffer == nullptr || p_size > UDPPacket::MAX_UDP_PKT_SIZE)
    return false;

  // Gather the self shared_ptr
  std::shared_ptr<UDPSocket> self = shared_from_this();

  // Atomically accept the send only if no other send operation is active
  bool operation_expected = false;
  if (!m_send_operation_active.compare_exchange_strong(operation_expected, true))
    return false;

  // From this point the operation is accepted, borrow the packet from the caller
  {
    std::scoped_lock<std::mutex> l(m_send_packet_mutex);
    m_send_packet = std::move(p_tx_buffer);
  }

  // Create the send function callback
  std::function<void(const asio::error_code &, std::size_t)> callback = [self](const asio::error_code &p_error, std::size_t) {
    // Ignore the send error TODO: think about it
    (void)p_error;

    // Take back the borrowed packet before allowing another send to start
    std::unique_ptr<UDPPacket> send_packet;
    {
      std::scoped_lock<std::mutex> l(self->m_send_packet_mutex);
      send_packet = std::move(self->m_send_packet);
    }

    // Restore the possibility of sending data packages
    self->m_send_operation_active = false;

    // Copy the callback while protected, then invoke user code without internal locks
    std::function<void(std::unique_ptr<UDPPacket>)> send_callback;
    {
      std::scoped_lock<std::mutex> l(self->m_send_callback_mutex);
      send_callback = self->m_send_callback;
    }

    // Call the user callback
    if (send_callback)
      send_callback(std::move(send_packet));
  };

  // Send the packet if possible
  try {
    std::scoped_lock<std::mutex> l(m_socket_mutex);
    m_socket.async_send_to(asio::buffer(m_send_packet->m_packet_data.data(), p_size), p_destination, callback);
  } catch (const std::exception &) {
    // Asio rejected the start, restore caller ownership and mark the operation as inactive
    {
      std::scoped_lock<std::mutex> l(m_send_packet_mutex);
      p_tx_buffer = std::move(m_send_packet);
    }

    // Restore the possibility of sending data packages
    m_send_operation_active = false;
    return false;
  }

  return true;
}

bool UDPSocket::asyncReceive(std::unique_ptr<UDPPacket> &p_rx_buffer) {
  // Invalid input is rejected before borrowing the packet, so the caller keeps ownership
  if (p_rx_buffer == nullptr)
    return false;

  // Gather the self shared_ptr
  std::shared_ptr<UDPSocket> self = shared_from_this();

  // Atomically accept the receive only if no other receive operation is active
  bool operation_expected = false;
  if (!m_receive_operation_active.compare_exchange_strong(operation_expected, true))
    return false;

  // Borrow the packet from the caller
  {
    std::scoped_lock<std::mutex> l(m_receive_packet_mutex);
    m_receive_packet = std::move(p_rx_buffer);
  }

  // Create the function callback
  std::function<void(const asio::error_code &, std::size_t)> callback = [self](const asio::error_code &p_error, std::size_t p_len) {
    // Take back the borrowed packet before allowing another receive to start
    std::unique_ptr<UDPPacket> receive_packet;
    {
      std::scoped_lock<std::mutex> l(self->m_receive_packet_mutex);
      receive_packet = std::move(self->m_receive_packet);
    }

    // Restore the possibility of receiveing data packages
    self->m_receive_operation_active = false;

    // Copy the callback while protected, then invoke user code without internal locks
    std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::error_code)> receive_callback;
    {
      std::scoped_lock<std::mutex> l(self->m_receive_callback_mutex);
      receive_callback = self->m_receive_callback;
    }

    // Call the user callback if present
    if (receive_callback)
      receive_callback(std::move(receive_packet), p_len, p_error);
  };

  // Receive the packet if possible
  try {
    std::scoped_lock<std::mutex> l(m_socket_mutex);
    m_socket.async_receive_from(asio::buffer(m_receive_packet->m_packet_data.data(), UDPPacket::MAX_UDP_PKT_SIZE), m_receive_endpoint, callback);
  } catch (const std::exception &e) {
    // Asio rejected the start, restore the caller ownership and mark the operation as inactive
    {
      std::scoped_lock<std::mutex> l(m_receive_packet_mutex);
      p_rx_buffer = std::move(m_receive_packet);
    }

    // Restore the possibility of receiving data packages
    m_receive_operation_active = false;
    return false;
  }

  return true;
}

} // namespace network
} // namespace cudp
