#pragma once

#include <asio.hpp>
#include <cstdint>
#include <functional>
#include <memory>

// Project libraries
#include <network/UDPPacket.hpp>

namespace cudp {
namespace network {

/**
 * @brief Common interface implemented by real and test UDP transceivers.
 * The interface deliberately preserves the public methods and signatures exposed by UDPSocket
 */
class Transceiver {
public:
  /**
   * @brief Destroys the transceiver through the common interface.
   */
  virtual ~Transceiver() = default;

  /**
   * @brief Closes the transceiver and prevents new asynchronous operations from being accepted.
   * Implementations may still invoke callbacks for operations accepted before the close request.
   * Calling this method on an already closed transceiver is considered successful
   *
   * @return True if the transceiver is already closed or has been closed successfully; false if
   * the close operation fails.
   */
  [[nodiscard]] virtual bool close() = 0;

  /**
   * @brief Starts an asynchronous send to the specified UDP destination.
   * When the operation is accepted, ownership of @p p_tx_buffer is transferred to the
   * transceiver and the caller's pointer becomes null. At completion, the same buffer is returned
   * through @p p_callback. If the operation is rejected, the caller retains ownership and the
   * callback is not invoked. Only one send operation may be pending at a time
   *
   * @param p_destination UDP endpoint to which the packet is sent
   * @param[in,out] p_tx_buffer Packet containing the bytes to send. It is moved from only when the operation is accepted
   * @param p_size Number of bytes to send from the packet, which must not exceed UDPPacket::MAX_UDP_PKT_SIZE
   * @param p_callback Completion function receiving the returned packet and the ASIO error code. The function may be empty
   * @return True if the operation has been accepted; false if an argument is invalid, the
   * transceiver is closed, or another send is already pending
   */
  [[nodiscard]] virtual bool asyncSend(asio::ip::udp::endpoint p_destination,
                                       std::unique_ptr<UDPPacket> &p_tx_buffer,
                                       uint32_t p_size,
                                       std::function<void(std::unique_ptr<UDPPacket>, asio::error_code)> p_callback) = 0;

  /**
   * @brief Starts an asynchronous receive from any UDP endpoint.
   * When the operation is accepted, ownership of @p p_rx_buffer is transferred to the
   * transceiver and the caller's pointer becomes null. At completion, the same buffer is returned
   * through @p p_callback together with the received byte count, source endpoint, and error code.
   * If the operation is rejected, the caller retains ownership and the callback is not invoked.
   * Only one receive operation may be pending at a time.
   *
   * @param[in,out] p_rx_buffer Packet used as the receive buffer. It is moved from only when the
   * operation is accepted
   * @param p_callback Completion function receiving the returned packet, number of valid bytes,
   * source UDP endpoint, and ASIO error code. The function may be empty
   * @return True if the operation has been accepted; false if the buffer is null, the transceiver
   * is closed, or another receive is already pending
   */
  [[nodiscard]] virtual bool
  asyncReceive(std::unique_ptr<UDPPacket> &p_rx_buffer,
               std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code)> p_callback) = 0;
};

} // namespace network
} // namespace cudp
