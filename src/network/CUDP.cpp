#include <network/CUDP.hpp>

#include <algorithm>
#include <exception>
#include <iostream>
#include <iterator>
#include <optional>
#include <utility>

namespace cudp {
namespace network {

CUDP::ConnectionState::ConnectionState(std::shared_ptr<ConnectionHandler> p_handler)
    : m_handler(std::move(p_handler)) {
  // Allocate the entire packet pool when the connection is accepted. No TX
  // packet allocation is therefore needed on the hot send path.
  for (std::unique_ptr<UDPPacket> &send_buffer : m_send_buffers) {
    send_buffer = std::make_unique<UDPPacket>();
  }
}

std::shared_ptr<CUDP> CUDP::create(asio::io_context &p_io_context,
                                   asio::ip::udp::endpoint p_local_endpoint,
                                   std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback) {
  // Construction and receive startup are deliberately separated because the
  // receive callback needs a valid weak reference to the completed object.
  std::shared_ptr<CUDP> multiplexer(new CUDP(p_io_context, std::move(p_local_endpoint), std::move(p_new_connection_callback)));
  multiplexer->startReceive();
  return multiplexer;
}

CUDP::CUDP(asio::io_context &p_io_context,
           asio::ip::udp::endpoint p_local_endpoint,
           std::function<void(asio::ip::udp::endpoint)> p_new_connection_callback)
    : m_socket(UDPSocket::create(p_io_context, p_local_endpoint))
    , m_new_connection_callback(std::move(p_new_connection_callback))
    , m_receive_packet(std::make_unique<UDPPacket>()) {}

CUDP::~CUDP() {
  // close() is idempotent. A weak reference is used by asynchronous callbacks,
  // so a pending operation does not prevent destruction of the multiplexer.
  static_cast<void>(close());
}

bool CUDP::registerConnection(asio::ip::udp::endpoint p_endpoint, std::shared_ptr<ConnectionHandler> p_handler) {
  if (p_handler == nullptr || m_closed) {
    return false;
  }

  // Allocate the sizeable per-connection pool outside the global state lock.
  // State is checked again afterwards because close or another registration
  // may have won the race while memory was being allocated.
  std::shared_ptr<ConnectionState> connection = std::make_shared<ConnectionState>(std::move(p_handler));

  std::scoped_lock l(m_state_mutex);
  if (m_closed || m_connections.find(p_endpoint) != m_connections.end()) {
    return false;
  }

  m_connections.emplace(p_endpoint, std::move(connection));

  // A directly registered endpoint is known too. If it is later unregistered,
  // subsequent datagrams are discarded rather than reported as a new peer.
  m_seen_endpoints.insert(std::move(p_endpoint));
  return true;
}

bool CUDP::unregisterConnection(const asio::ip::udp::endpoint &p_endpoint) {
  std::scoped_lock l(m_state_mutex);
  const size_t removed_connections = m_connections.erase(p_endpoint);

  // Connection insertions/removals change map indices. Keeping the scheduler
  // index in range is sufficient to preserve round-robin progress.
  if (m_connections.empty()) {
    m_round_robin_index = 0U;
  } else {
    m_round_robin_index %= m_connections.size();
  }

  return removed_connections != 0U;
}

bool CUDP::send(const asio::ip::udp::endpoint &p_destination, const uint8_t *p_data, uint32_t p_size) {
  if (p_data == nullptr || p_size > UDPPacket::MAX_UDP_PKT_SIZE || m_closed) {
    return false;
  }

  // A send call is also a protocol progress point: retry any packet retained
  // after a previously rejected asynchronous submission before queuing new data.
  trySendNext();

  {
    // The global-to-connection lock order is shared with the scheduler and TX
    // completion path. Besides preventing deadlocks, retaining the state lock
    // here prevents unregisterConnection() from accepting and then silently
    // losing a packet on a connection that has just been removed.
    std::scoped_lock l(m_state_mutex);
    if (m_closed) {
      return false;
    }

    const auto connection_it = m_connections.find(p_destination);
    if (connection_it == m_connections.end()) {
      return false;
    }

    const std::shared_ptr<ConnectionState> connection = connection_it->second;
    std::scoped_lock l(connection->m_mutex);

    const uint32_t buffer_index = connection->m_next_send_buffer;

    // A null slot is currently owned by UDPSocket. In that case the pool is
    // saturated even if the metadata queue itself has one free position.
    if (connection->m_send_buffers[buffer_index] == nullptr) {
      return false;
    }

    const QueuedPacket pending_packet{ buffer_index, p_size };

    // Reserve the queue position before touching the packet. When the queue is
    // full, the indexed buffer still belongs to an older queued message and
    // must not be overwritten.
    if (!connection->m_pending_packets.tryEnqueue(pending_packet)) {
      return false;
    }

    // Only the message bytes are copied. Queue scheduling subsequently moves
    // unique ownership of this same packet object into and out of UDPSocket.
    std::copy_n(p_data, static_cast<size_t>(p_size), connection->m_send_buffers[buffer_index]->m_packet_data.begin());
    connection->m_next_send_buffer = (buffer_index + 1U) % CONNECTION_BUFFER_COUNT;
  }

  // Starting I/O outside the enqueue critical section keeps all user-facing
  // operations responsive. m_send_active serializes competing schedulers.
  trySendNext();
  return true;
}

bool CUDP::close() {
  // Serialize the transition so a failed socket close can be retried without a
  // concurrent caller incorrectly observing an already completed close.
  std::scoped_lock l(m_state_mutex);
  if (m_closed) {
    return true;
  }

  m_closed                = true;
  const bool close_result = m_socket->close();
  if (!close_result) {
    m_closed = false;
  }

  return close_result;
}

void CUDP::startReceive() {
  if (m_closed) {
    return;
  }

  std::scoped_lock l(m_receive_mutex);
  if (m_closed || m_receive_packet == nullptr) {
    return;
  }

  // The callback keeps only a weak reference. This avoids making the permanent
  // receive loop a self-ownership cycle and allows the destructor to close the
  // socket when the last external shared pointer is released.
  const std::weak_ptr<CUDP> weak_self = weak_from_this();
  static_cast<void>(m_socket->asyncReceive(m_receive_packet, [weak_self](std::unique_ptr<UDPPacket> p_packet, uint32_t p_size,
                                                                         asio::ip::udp::endpoint p_source, asio::error_code p_error) {
    const std::shared_ptr<CUDP> self = weak_self.lock();
    if (self != nullptr) {
      self->handleReceive(std::move(p_packet), p_size, std::move(p_source), p_error);
    }
  }));
}

void CUDP::handleReceive(std::unique_ptr<UDPPacket> p_packet, uint32_t p_size, asio::ip::udp::endpoint p_source, asio::error_code p_error) {
  std::shared_ptr<ConnectionHandler> handler;
  bool notify_new_connection = false;

  // Failed receives do not contain a message to dispatch. Other transient UDP
  // errors still rearm reception below, unless close() has stopped the object.
  if (!p_error && p_packet != nullptr && p_size <= UDPPacket::MAX_UDP_PKT_SIZE && !m_closed) {
    {
      std::scoped_lock l(m_state_mutex);
      const auto connection_it = m_connections.find(p_source);

      if (connection_it != m_connections.end()) {
        handler = connection_it->second->m_handler;
      } else {
        // Only the first datagram from an unknown endpoint produces a callback.
        // The endpoint remains seen if the application chooses not to accept it.
        notify_new_connection = m_seen_endpoints.insert(p_source).second;
      }
    }

    if (notify_new_connection && m_new_connection_callback) {
      try {
        m_new_connection_callback(p_source);
      } catch (const std::exception &exception) {
        std::cerr << "[CUDP] The new-connection callback failed with an exception: " << exception.what() << '\n';
      } catch (...) {
        std::cerr << "[CUDP] The new-connection callback failed with an unknown exception" << '\n';
      }

      // The callback is allowed to synchronously accept the endpoint. Resolve
      // the handler only after it returns so the triggering datagram is kept.
      std::scoped_lock l(m_state_mutex);
      const auto connection_it = m_connections.find(p_source);
      if (connection_it != m_connections.end()) {
        handler = connection_it->second->m_handler;
      }
    }

    // No internal mutex is held while invoking application code. The receive
    // packet remains local, so ASIO cannot overwrite its bytes during the call.
    if (handler != nullptr && !m_closed) {
      try {
        handler->onMessage(p_source, p_packet->m_packet_data.data(), p_size);
      } catch (const std::exception &exception) {
        std::cerr << "[CUDP] The connection message callback failed with an exception: " << exception.what() << '\n';
      } catch (...) {
        std::cerr << "[CUDP] The connection message callback failed with an unknown exception" << '\n';
      }
    }
  }

  {
    // Reclaim ownership only after every consumer has finished reading the
    // datagram. startReceive() will transfer the same buffer back to UDPSocket.
    std::scoped_lock l(m_receive_mutex);
    m_receive_packet = std::move(p_packet);
  }

  startReceive();
}

void CUDP::trySendNext() {
  asio::ip::udp::endpoint destination;
  std::shared_ptr<ConnectionState> selected_connection;
  std::unique_ptr<UDPPacket> packet;
  QueuedPacket selected_packet;

  {
    std::scoped_lock l(m_state_mutex);
    if (m_closed || m_send_active || m_connections.empty()) {
      return;
    }

    const size_t connection_count = m_connections.size();
    const size_t first_index      = m_round_robin_index % connection_count;
    auto connection_it            = m_connections.begin();
    std::advance(connection_it, static_cast<std::ptrdiff_t>(first_index));

    // Inspect every endpoint at most once. Empty queues are skipped and the map
    // iterator wraps around, producing fair progress between busy connections.
    for (size_t checked_connections = 0; checked_connections < connection_count; checked_connections++) {
      const size_t selected_index                       = (first_index + checked_connections) % connection_count;
      const std::shared_ptr<ConnectionState> connection = connection_it->second;
      std::scoped_lock connection_lock(connection->m_mutex);
      const std::optional<QueuedPacket> queued_packet = connection->m_pending_packets.dequeue();

      if (queued_packet.has_value()) {
        selected_packet = queued_packet.value();

        // Queue entries and packet slots are changed under the same connection
        // lock. A broken invariant is discarded defensively instead of passing
        // a null or out-of-range buffer to the socket.
        if (selected_packet.m_buffer_index < CONNECTION_BUFFER_COUNT && connection->m_send_buffers[selected_packet.m_buffer_index] != nullptr) {
          destination         = connection_it->first;
          packet              = std::move(connection->m_send_buffers[selected_packet.m_buffer_index]);
          selected_connection = connection;
          m_round_robin_index = (selected_index + 1U) % connection_count;
          m_send_active       = true;
          break;
        }
      }

      connection_it++;
      if (connection_it == m_connections.end()) {
        connection_it = m_connections.begin();
      }
    }
  }

  if (selected_connection == nullptr || packet == nullptr) {
    return;
  }

  const std::weak_ptr<CUDP> weak_self = weak_from_this();
  const bool send_started             = m_socket->asyncSend(
      destination, packet, selected_packet.m_size,
      [weak_self, selected_connection, buffer_index = selected_packet.m_buffer_index](std::unique_ptr<UDPPacket> p_packet, asio::error_code p_error) {
        const std::shared_ptr<CUDP> self = weak_self.lock();
        if (self != nullptr) {
          self->handleSend(selected_connection, buffer_index, std::move(p_packet), p_error);
        }
      });

  if (send_started) {
    return;
  }

  // A rejected async operation does not take ownership. Put both packet and
  // metadata back so a later protocol event can retry without reallocating.
  // No immediate recursive retry is made, avoiding a busy loop on a closed or
  // otherwise unavailable socket.
  std::scoped_lock l(m_state_mutex);
  std::scoped_lock connection_lock(selected_connection->m_mutex);
  selected_connection->m_send_buffers[selected_packet.m_buffer_index] = std::move(packet);
  if (!m_closed) {
    static_cast<void>(selected_connection->m_pending_packets.tryEnqueue(selected_packet));
  }
  m_send_active = false;
}

void CUDP::handleSend(std::shared_ptr<ConnectionState> p_connection,
                      uint32_t p_buffer_index,
                      std::unique_ptr<UDPPacket> p_packet,
                      asio::error_code p_error) {
  {
    // Follow the global-to-connection lock order used everywhere else. The
    // returned packet becomes reusable before another scheduler is enabled.
    std::scoped_lock l(m_state_mutex);
    std::scoped_lock connection_lock(p_connection->m_mutex);

    if (p_buffer_index < CONNECTION_BUFFER_COUNT && p_packet != nullptr) {
      p_connection->m_send_buffers[p_buffer_index] = std::move(p_packet);
    }
    m_send_active = false;
  }

  // UDP completion errors consume the datagram just like successful sends;
  // reliability or retransmission belongs to the higher ConnectionHandler.
  static_cast<void>(p_error);
  trySendNext();
}

} // namespace network
} // namespace cudp
