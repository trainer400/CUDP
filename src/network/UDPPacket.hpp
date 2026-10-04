#pragma once

#include <array>
#include <cstdint>
#include <tuple>

namespace cudp {
namespace network {

struct UDPPacket {
  static constexpr uint32_t MAX_UDP_PKT_SIZE = 65536;

  // Actual data buffer
  std::array<uint8_t, MAX_UDP_PKT_SIZE> m_packet_data = {};
  size_t m_data_size                                  = 0;
};

} // namespace network
} // namespace cudp