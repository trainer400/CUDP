#pragma once

#include <cstdint>
#include <array>
#include <tuple>

namespace cudp {
namespace network {

struct UDPPacket {
  static constexpr uint32_t MAX_UDP_PKT_SIZE = 65536;

  // Actual data buffer
  std::array<uint8_t, MAX_UDP_PKT_SIZE> m_packet_data = {};
};

} // namespace network
} // namespace cudp