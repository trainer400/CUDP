#pragma once

#include <array>
#include <asio.hpp>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <map>
#include <memory>
#include <mutex>
#include <set>

// Project libraries
#include <data/CircularBuffer.hpp>
#include <network/ConnectionHandler.hpp>
#include <network/Transceiver.hpp>
#include <network/UDPPacket.hpp>

namespace cudp {
namespace network {

/**
 * @brief Thread-safe CUDP connection multiplexer over a single UDP socket.
 * Protocol activity advances only when send() is called or a UDP operation
 * completes. User callbacks are invoked without holding internal locks and may
 * therefore call registerConnection(), unregisterConnection(), or send()
 */
class CUDP final : public std::enable_shared_from_this<CUDP> {
public:
  // Number of queued packet buffers reserved for each connection
  static constexpr uint32_t CONNECTION_BUFFER_COUNT = 32U;

  /**
   * @brief Creates the CUDP multiplexer and starts its continuous asynchronous receive
   * @param transceiver Transceiver that will be used by the multiplexer object to communicate
   * @param p_new_connection_callback Function notified with a previously unseen endpoint
   * @return Shared pointer owning the created multiplexer
   *
   * The callback may synchronously register the endpoint. In that case, the
   * datagram that caused the notification is delivered to the new handler.
   */
  [[nodiscard]] static std::optional<std::shared_ptr<CUDP>> create(std::unique_ptr<Transceiver> p_transceiver,
                                                                   std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback);

  /**
   *  @brief Closes the socket and rejects subsequent sends.
   */
  ~CUDP();

  /**
   * @brief Registers the handler and fixed transmit buffers for an endpoint
   * @param p_endpoint Remote endpoint associated with the connection
   * @param p_handler User-owned connection handler shared with the multiplexer
   * @return False if the handler is null, the endpoint is already registered, or CUDP is closed
   */
  [[nodiscard]] bool registerConnection(const asio::ip::udp::endpoint& p_endpoint, std::shared_ptr<ConnectionHandler> p_handler);

  /**
   * @brief Removes a registered connection and discards its queued messages
   * @param p_endpoint Endpoint to remove
   * @return True if a connection was removed
   */
  [[nodiscard]] bool unregisterConnection(const asio::ip::udp::endpoint &p_endpoint);

  /**
   * @brief Copies and queues a message for round-robin transmission
   * @param p_destination Endpoint of a registered connection
   * @param p_data Bytes to copy into a connection packet buffer
   * @param p_size Number of bytes to send
   * @return False if the arguments are invalid, the connection is absent, the
   * queue is full, or the multiplexer is closed
   */
  [[nodiscard]] bool send(const asio::ip::udp::endpoint &p_destination, const uint8_t *p_data, uint32_t p_size);

  /**
   * @brief Closes the owned UDP socket
   * @return True if the socket is already closed or closes successfully
   */
  [[nodiscard]] bool close();

private:
  // Entry that represents an enqueued packet ready to be sent. The index
  // refers to the std::array memory that stores all the tx buffers
  struct QueuedPacket {
    uint32_t m_buffer_index = 0U;
    uint32_t m_size         = 0U;
  };

  // Single connection entry with handler and buffers
  struct ConnectionState {
    /**
     * @brief Creates the fixed queue and packet pool associated with one handler.
     */
    explicit ConnectionState(std::shared_ptr<ConnectionHandler> p_handler);

    // Connection handler that corresponds to the multiplexed endpoint
    std::shared_ptr<ConnectionHandler> m_handler;
    std::mutex m_mutex;

    // TX data buffers that manage the order in which the packets have to be delivered and their memory addresses
    std::array<std::unique_ptr<UDPPacket>, CONNECTION_BUFFER_COUNT> m_send_buffers;
    data::CircularBuffer<QueuedPacket> m_pending_packets{ CONNECTION_BUFFER_COUNT };
    uint32_t m_next_send_buffer = 0U;
  };

  /**
   * @brief Constructs the multiplexer; create() completes asynchronous setup
   */
  CUDP(std::unique_ptr<Transceiver> p_transceiver, std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback);

  /**
   * @brief Submits the owned receive buffer to the UDP socket
   */
  void startReceive();

  /**
   *  @brief Processes one receive completion and rearms reception
   */
  void handleReceive(std::unique_ptr<UDPPacket> p_packet, uint32_t p_size, asio::ip::udp::endpoint p_source, asio::error_code p_error);

  /**
   * @brief Starts the next fair transmission when the socket is idle.
   * The send operation might fail in presence of already pending TX packets
   */
  void trySendNext();

  // Thread safe socket that needs multiplexing
  std::unique_ptr<Transceiver> m_socket;
  std::atomic<bool> m_closed = false;

  // User connection callback that needs to be called when a new connection appears
  std::function<void(asio::ip::udp::endpoint)> m_new_connection_callback;

  // Internal connection states
  std::mutex m_state_mutex;
  std::map<asio::ip::udp::endpoint, std::shared_ptr<ConnectionState>> m_connections;

  // Set of seen endpoints that need to be consulted when receiving a packet.
  // If it has already been seen (but not registered as a connection), the packet will be discarded.
  // TODO: implement a timer based persistence (after a certain amount, the endpoint will be discarded)
  std::set<asio::ip::udp::endpoint> m_seen_endpoints;

  // Round robin index that indicates the last connection that has sent data
  std::size_t m_round_robin_index = 0U;
  bool m_send_active              = false;

  // RX buffer and associated mutex
  std::mutex m_receive_mutex;
  std::unique_ptr<UDPPacket> m_receive_packet;
};

} // namespace network
} // namespace cudp
