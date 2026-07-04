#pragma once

#include <asio.hpp>

// Project libraries
#include <network/UDPPacket.hpp>

namespace cudp {
namespace network {

/**
 * @brief Thread safe class that enables the user to manage the asio udp socket. It interfaces
 * with the context and the async calls to expose only what is really needed to the upper levels.
 * To be extra safe with the memory buffer, the class takes ownership of the read/write buffers
 * and returns it to the owner only when the read/write operation is completed.
 *
 * Additionally, the class forces the user to deal one by one message. The class does not
 * own a queue on purpose, so that the user has to maintain and manage its own
 */
class UDPSocket : public std::enable_shared_from_this<UDPSocket> {
public:
  /**
   * @brief Constructs a UDPSocket object with the asio io context and the specified endpoint
   */
  UDPSocket(asio::io_context &p_io_context, asio::ip::udp::endpoint p_endpoint);

  /**
   * @brief Sets the function that is called on send success
   * @param p_callback The function that needs to be called on "send" completion.
   * The function must accept the previously borrowed buffer in return
   */
  void setOnSendCallback(std::function<void(std::unique_ptr<UDPPacket>)> p_callback);

  /**
   * @brief Sets the function that is called when a packet is received
   * @param p_callback The function that needs to be called on "receive" completion.
   * The function must accept the previously borrowed buffer in return
   */
  void setOnReceiveCallback(std::function<void(std::unique_ptr<UDPPacket>, uint32_t)> p_callback);

  /**
   * @brief Send to the specified destination endpoint a packet. The tx buffer is borrowed from the user
   * and will be returned once the asio send call has been completed.
   * @warning The operation may fail if the user inputs are not valid or if another send call is still pending
   *
   * @param p_destination The message destination endpoint
   * @param p_tx_buffer The buffer to be used to take the data to transmit
   * @param p_size The amount of data to be sent (in bytes)
   * @return Result of the async send operation
   */
  [[nodiscard]] bool asyncSend(asio::ip::udp::endpoint p_destination, std::unique_ptr<UDPPacket> p_tx_buffer, uint32_t p_size);

  /**
   * @brief Receive from any endpoint into the specified buffer. The rx buffer is borrowed from the user
   * and will be returned once the asio receive call has been completed.
   * @warning The operation may fail if the user inputs are not valid of if another receive call is still pending
   *
   * @param p_rx_buffer The buffer to be used when writing received data
   * @return Result of the async receive operation
   */
  [[nodiscard]] bool asyncReceive(std::unique_ptr<UDPPacket> p_rx_buffer);

private:
  // ASIO io context and endpoint
  asio::io_context &m_io_context;
  asio::ip::udp::endpoint m_endpoint;

  // UDP socket
  asio::ip::udp m_socket;

  // Borrowed receive/send buffers
  std::unique_ptr<UDPPacket> m_receive_packet = nullptr;
  std::unique_ptr<UDPPacket> m_send_packet    = nullptr;

  // Receive/send operations active booleans. Those ensure that the user
  // cannot start simultaneously the same operation
  std::atomic<bool> m_send_operation_active    = false;
  std::atomic<bool> m_receive_operation_active = false;

  // Receive/send callback functions
  std::function<void(std::unique_ptr<UDPPacket>)> m_send_callback;
  std::function<void(std::unique_ptr<UDPPacket>, uint32_t)> m_receive_callback;

  // Callbacks synchronization mutexes
  std::mutex m_send_callback_mutex;
  std::mutex m_receive_callback_mutex;
};

} // namespace network
} // namespace cudp