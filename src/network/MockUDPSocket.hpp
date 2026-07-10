#pragma once

#include <functional>
#include <memory>
#include <mutex>

// Project libraries
#include <network/Transceiver.hpp>

namespace cudp {
namespace network {

/**
 * @brief Controllable Transceiver implementation intended exclusively for tests.
 * asyncSend() and asyncReceive() retain their buffer and callback without performing network I/O.
 * A test completes an operation explicitly through triggerSend() or triggerReceive(). The trigger
 * invokes the corresponding callback synchronously on the calling thread. Consequently this mock
 * does not own or call an ASIO io_context, executor, or socket
 */
class MockUDPSocket final : public Transceiver {
public:
  MockUDPSocket() = default;

  [[nodiscard]] bool close() override;

  [[nodiscard]] bool asyncSend(asio::ip::udp::endpoint p_destination,
                               std::unique_ptr<UDPPacket> &p_tx_buffer,
                               uint32_t p_size,
                               std::function<void(std::unique_ptr<UDPPacket>, asio::error_code)> p_callback) override;

  [[nodiscard]] bool
  asyncReceive(std::unique_ptr<UDPPacket> &p_rx_buffer,
               std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code)> p_callback) override;

  /**
   * @brief Completes the pending send synchronously on the thread calling this method
   * @return False if there is no pending send; true if the operation has been completed
   */
  [[nodiscard]] bool triggerSend(asio::error_code p_error = {});

  /**
   * @brief Completes the pending receive synchronously on the thread calling this method
   * @param p_size Number of valid bytes already present in the borrowed receive packet
   * @param p_source Endpoint supplied to the receive callback
   * @param p_error Error code supplied to the receive callback
   * @return False if there is no pending receive or p_size is invalid; true otherwise
   */
  [[nodiscard]] bool triggerReceive(uint32_t p_size, asio::ip::udp::endpoint p_source = {}, asio::error_code p_error = {});

private:
  std::mutex m_mutex;
  bool m_closed          = false;
  bool m_send_pending    = false;
  bool m_receive_pending = false;

  std::unique_ptr<UDPPacket> m_send_packet;
  std::unique_ptr<UDPPacket> m_receive_packet;
  std::function<void(std::unique_ptr<UDPPacket>, asio::error_code)> m_send_callback;
  std::function<void(std::unique_ptr<UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code)> m_receive_callback;
};

} // namespace network
} // namespace cudp
