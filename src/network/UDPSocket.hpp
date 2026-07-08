#pragma once

#include <asio.hpp>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

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
   * @brief Creates a UDPSocket object owned by a shared pointer
   *
   * UDPSocket asynchronous operations capture shared_from_this(), so the socket
   * must be owned by a std::shared_ptr before any operation is started
   *
   * @param p_io_context The asio io context used by the UDP socket
   * @param p_endpoint The local endpoint used to bind the UDP socket
   * @return Shared pointer owning the created UDP socket
   */
  [[nodiscard]] static std::shared_ptr<UDPSocket> create(asio::io_context &p_io_context, asio::ip::udp::endpoint p_endpoint) {
    return std::shared_ptr<UDPSocket>(new UDPSocket(p_io_context, p_endpoint));
  }

  /**
   * @brief Sets the function that is called on send completion
   * @param p_callback The function that needs to be called on "send" completion.
   * The function must accept the previously borrowed buffer in return and the
   * asio error code produced by the operation
   */
  void setOnSendCallback(std::function<void(std::unique_ptr<UDPPacket>, asio::error_code)> p_callback);

  /**
   * @brief Sets the function that is called when a packet is received
   * @param p_callback The function that needs to be called on "receive" completion.
   * The function must accept the previously borrowed buffer in return, the amount
   * of valid received bytes and the asio error code produced by the operation
   */
  void setOnReceiveCallback(std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::error_code)> p_callback);

  /**
   * @brief Closes the UDP socket and cancels any pending asynchronous operation.
   * Pending operation callbacks may still be invoked with the asio error code produced
   * by the close operation
   *
   * @return True if the socket is already closed or has been closed successfully, false otherwise
   */
  [[nodiscard]] bool close();

  /**
   * @brief Send to the specified destination endpoint a packet. The tx buffer is borrowed from the user
   * and will be returned once the asio send call has been completed.
   * If the operation cannot be started, the buffer remains owned by the caller and no callback is invoked.
   * @warning The operation may fail if the user inputs are not valid or if another send call is still pending
   *
   * @param p_destination The message destination endpoint
   * @param p_tx_buffer The buffer to be used to take the data to transmit. The pointer is emptied only
   * if the operation is accepted
   * @param p_size The amount of data to be sent (in bytes)
   * @return Result of the async send operation
   */
  [[nodiscard]] bool asyncSend(asio::ip::udp::endpoint p_destination, std::unique_ptr<UDPPacket> &p_tx_buffer, uint32_t p_size);

  /**
   * @brief Receive from any endpoint into the specified buffer. The rx buffer is borrowed from the user
   * and will be returned once the asio receive call has been completed.
   * If the operation cannot be started, the buffer remains owned by the caller and no callback is invoked.
   * @warning The operation may fail if the user inputs are not valid of if another receive call is still pending
   *
   * @param p_rx_buffer The buffer to be used when writing received data. The pointer is emptied only
   * if the operation is accepted
   * @return Result of the async receive operation
   */
  [[nodiscard]] bool asyncReceive(std::unique_ptr<UDPPacket> &p_rx_buffer);

private:
  /**
   * @brief Constructs a UDPSocket object with the asio io context and the specified endpoint.
   * Private method to be used only by the factory
   */
  UDPSocket(asio::io_context &p_io_context, asio::ip::udp::endpoint p_endpoint);

  // ASIO io context and endpoints
  asio::io_context &m_io_context;
  asio::ip::udp::endpoint m_endpoint;
  asio::ip::udp::endpoint m_receive_endpoint;

  // UDP socket
  asio::ip::udp::socket m_socket;

  // Borrowed receive/send buffers
  std::unique_ptr<UDPPacket> m_receive_packet = nullptr;
  std::unique_ptr<UDPPacket> m_send_packet    = nullptr;

  // Receive/send operations active booleans. Those ensure that the user
  // cannot start simultaneously the same operation
  std::atomic<bool> m_send_operation_active    = false;
  std::atomic<bool> m_receive_operation_active = false;

  // Receive/send callback functions
  std::function<void(std::unique_ptr<UDPPacket>, asio::error_code)> m_send_callback;
  std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::error_code)> m_receive_callback;

  // Callbacks synchronization mutexes
  std::mutex m_send_callback_mutex;
  std::mutex m_receive_callback_mutex;

  // Borrowed buffers synchronization mutexes
  std::mutex m_send_packet_mutex;
  std::mutex m_receive_packet_mutex;

  // Socket synchronization mutex
  std::mutex m_socket_mutex;
};

} // namespace network
} // namespace cudp
