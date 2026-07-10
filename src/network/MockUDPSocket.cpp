#include <network/MockUDPSocket.hpp>

#include <iostream>
#include <utility>

namespace cudp {
namespace network {

bool MockUDPSocket::close() {
  std::scoped_lock<std::mutex> lock(m_mutex);
  m_closed = true;
  return true;
}

bool MockUDPSocket::asyncSend(asio::ip::udp::endpoint,
                              std::unique_ptr<UDPPacket> &p_tx_buffer,
                              uint32_t p_size,
                              std::function<void(std::unique_ptr<UDPPacket>, asio::error_code)> p_callback) {
  // "Enqueue" the async send operation only if the user passed buffer is valid
  if (p_tx_buffer == nullptr || p_size > UDPPacket::MAX_UDP_PKT_SIZE)
    return false;

  // "Enqueue" the async send if the socket is valid and no other send async is pending
  std::scoped_lock<std::mutex> lock(m_mutex);
  if (m_closed || m_send_pending)
    return false;

  // Under lock, save all the needed data to call the async send callback
  m_send_packet   = std::move(p_tx_buffer);
  m_send_callback = std::move(p_callback);
  m_send_pending  = true;
  return true;
}

bool MockUDPSocket::asyncReceive(std::unique_ptr<UDPPacket> &p_rx_buffer,
                                 std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code)> p_callback) {
  // "Enqueue" the async receive operation only the user passed a valid buffer
  if (p_rx_buffer == nullptr)
    return false;

  // "Enqueue" the async receive only if the socket is valid and no other receive is pending
  std::scoped_lock<std::mutex> lock(m_mutex);
  if (m_closed || m_receive_pending)
    return false;

  // Under lock, save all the needed data to call the async receive callback
  m_receive_packet   = std::move(p_rx_buffer);
  m_receive_callback = std::move(p_callback);
  m_receive_pending  = true;
  return true;
}

bool MockUDPSocket::triggerSend(asio::error_code p_error) {
  std::unique_ptr<UDPPacket> packet;
  
  // Copy the callback under lock if a send operation is pending
  std::function<void(std::unique_ptr<UDPPacket>, asio::error_code)> callback;
  {
    std::scoped_lock<std::mutex> lock(m_mutex);
    if (!m_send_pending)
      return false;

    packet         = std::move(m_send_packet);
    callback       = std::move(m_send_callback);
    m_send_pending = false;
  }

  // Call the callback in a protected environment
  if (callback) {
    try {
      callback(std::move(packet), p_error);
    } catch (const std::exception &e) {
      std::cerr << "[MockUDPSocket] The user send-callback failed with an exception: " << e.what() << '\n';
    } catch (...) {
      std::cerr << "[MockUDPSocket] The user send-callback failed with an unknown exception" << '\n';
    }
  }
  return true;
}

bool MockUDPSocket::triggerReceive(uint32_t p_size, asio::ip::udp::endpoint p_source, asio::error_code p_error) {
  if (p_size > UDPPacket::MAX_UDP_PKT_SIZE)
    return false;

  std::unique_ptr<UDPPacket> packet;

  // Copy the callback under lock if a receive operation is pending
  std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code)> callback;
  {
    std::scoped_lock<std::mutex> lock(m_mutex);
    if (!m_receive_pending)
      return false;

    packet            = std::move(m_receive_packet);
    callback          = std::move(m_receive_callback);
    m_receive_pending = false;
  }

  // Call the callback in a protected environment
  if (callback) {
    try {
      callback(std::move(packet), p_size, std::move(p_source), p_error);
    } catch (const std::exception &e) {
      std::cerr << "[MockUDPSocket] The user receive-callback failed with an exception: " << e.what() << '\n';
    } catch (...) {
      std::cerr << "[MockUDPSocket] The user receive-callback failed with an unknown exception" << '\n';
    }
  }
  return true;
}

} // namespace network
} // namespace cudp
