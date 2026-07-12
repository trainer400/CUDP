#pragma once

#include <asio.hpp>
#include <cstdint>

namespace cudp {
namespace network {

/**
 * @brief Abstract interface for a connection object that implements
 * different logics of reliability and congestion control. The class
 * serves as a template for the CUDP multiplexer to handle the connections
 */
class ConnectionHandler {
public:
  /**
   * @brief Enables safe destruction through the abstract interface
   */
  virtual ~ConnectionHandler() = default;

  /**
   * @brief Receives one message from the associated connection
   * @param p_source Source endpoint associated with the handler
   * @param p_data Message bytes, valid only for the duration of this call
   * @param p_size Number of valid message bytes
   */
  virtual void onMessage(const asio::ip::udp::endpoint &p_source, const uint8_t *p_data, uint32_t p_size) = 0;
};

} // namespace network
} // namespace cudp
