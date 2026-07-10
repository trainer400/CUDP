#include <catch2/catch_test_macros.hpp>

#include <network/MockUDPSocket.hpp>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <type_traits>

static_assert(std::is_base_of_v<cudp::network::Transceiver, cudp::network::MockUDPSocket>);

static std::unique_ptr<cudp::network::UDPPacket> makePacket(uint8_t p_first_byte = 0) {
  std::unique_ptr<cudp::network::UDPPacket> packet = std::make_unique<cudp::network::UDPPacket>();
  packet->m_packet_data[0]                         = p_first_byte;
  return packet;
}

static asio::ip::udp::endpoint makeEndpoint(uint16_t p_port = 4242) { return { asio::ip::address_v4::loopback(), p_port }; }

TEST_CASE("MockUDPSocket is usable through Transceiver", "[MockUDPSocket][interface]") {
  std::unique_ptr<cudp::network::Transceiver> transceiver = std::make_unique<cudp::network::MockUDPSocket>();

  REQUIRE(transceiver->close());
}

TEST_CASE("MockUDPSocket close is idempotent", "[MockUDPSocket][close]") {
  cudp::network::MockUDPSocket socket;

  REQUIRE(socket.close());
  REQUIRE(socket.close());
}

TEST_CASE("MockUDPSocket close rejects a new send without taking its packet", "[MockUDPSocket][close][send]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  cudp::network::UDPPacket *packet_address         = packet.get();
  REQUIRE(socket.close());

  REQUIRE_FALSE(socket.asyncSend(makeEndpoint(), packet, 1, {}));
  REQUIRE(packet.get() == packet_address);
}

TEST_CASE("MockUDPSocket close rejects a new receive without taking its packet", "[MockUDPSocket][close][receive]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  cudp::network::UDPPacket *packet_address         = packet.get();
  REQUIRE(socket.close());

  REQUIRE_FALSE(socket.asyncReceive(packet, {}));
  REQUIRE(packet.get() == packet_address);
}

TEST_CASE("MockUDPSocket close leaves an accepted send triggerable", "[MockUDPSocket][close][send]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  bool callback_called                             = false;
  REQUIRE(socket.asyncSend(makeEndpoint(), packet, 1,
                           [&callback_called](std::unique_ptr<cudp::network::UDPPacket>, asio::error_code) { callback_called = true; }));

  REQUIRE(socket.close());
  REQUIRE_FALSE(callback_called);
  REQUIRE(socket.triggerSend());
  REQUIRE(callback_called);
}

TEST_CASE("MockUDPSocket close leaves an accepted receive triggerable", "[MockUDPSocket][close][receive]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  bool callback_called                             = false;
  REQUIRE(socket.asyncReceive(packet, [&callback_called](std::unique_ptr<cudp::network::UDPPacket>, uint32_t, asio::ip::udp::endpoint,
                                                         asio::error_code) { callback_called = true; }));

  REQUIRE(socket.close());
  REQUIRE_FALSE(callback_called);
  REQUIRE(socket.triggerReceive(1));
  REQUIRE(callback_called);
}

TEST_CASE("MockUDPSocket rejects a send with a null packet", "[MockUDPSocket][send][validation]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet;

  REQUIRE_FALSE(socket.asyncSend(makeEndpoint(), packet, 1, {}));
  REQUIRE(packet == nullptr);
}

TEST_CASE("MockUDPSocket rejects a send larger than packet capacity", "[MockUDPSocket][send][validation]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  cudp::network::UDPPacket *packet_address         = packet.get();

  REQUIRE_FALSE(socket.asyncSend(makeEndpoint(), packet, cudp::network::UDPPacket::MAX_UDP_PKT_SIZE + 1, {}));
  REQUIRE(packet.get() == packet_address);
}

TEST_CASE("MockUDPSocket accepts a zero length send", "[MockUDPSocket][send][boundary]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();

  REQUIRE(socket.asyncSend(makeEndpoint(), packet, 0, {}));
  REQUIRE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket accepts a send equal to packet capacity", "[MockUDPSocket][send][boundary]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();

  REQUIRE(socket.asyncSend(makeEndpoint(), packet, cudp::network::UDPPacket::MAX_UDP_PKT_SIZE, {}));
  REQUIRE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket rejects a second pending send without taking its packet", "[MockUDPSocket][send][pending]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> first_packet  = makePacket();
  std::unique_ptr<cudp::network::UDPPacket> second_packet = makePacket();
  cudp::network::UDPPacket *second_packet_address         = second_packet.get();
  REQUIRE(socket.asyncSend(makeEndpoint(), first_packet, 1, {}));

  REQUIRE_FALSE(socket.asyncSend(makeEndpoint(), second_packet, 1, {}));
  REQUIRE(second_packet.get() == second_packet_address);
  REQUIRE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket send returns the same packet and trigger error", "[MockUDPSocket][send][callback]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket(42);
  cudp::network::UDPPacket *packet_address         = packet.get();
  bool callback_called                             = false;
  asio::error_code expected_error                  = asio::error::connection_refused;
  REQUIRE(socket.asyncSend(makeEndpoint(), packet, 1, [&](std::unique_ptr<cudp::network::UDPPacket> p_packet, asio::error_code p_error) {
    callback_called = true;
    CHECK(p_packet.get() == packet_address);
    CHECK(p_packet->m_packet_data[0] == 42);
    CHECK(p_error == expected_error);
  }));

  REQUIRE(packet == nullptr);
  REQUIRE_FALSE(callback_called);
  REQUIRE(socket.triggerSend(expected_error));
  REQUIRE(callback_called);
}

TEST_CASE("MockUDPSocket triggerSend returns false without a pending send", "[MockUDPSocket][send][trigger]") {
  cudp::network::MockUDPSocket socket;

  REQUIRE_FALSE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket triggerSend accepts an empty callback", "[MockUDPSocket][send][callback]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  REQUIRE(socket.asyncSend(makeEndpoint(), packet, 1, {}));

  REQUIRE(socket.triggerSend());
  REQUIRE_FALSE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket send callback runs on the trigger thread", "[MockUDPSocket][send][thread]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  std::thread::id callback_thread;
  std::thread::id trigger_thread;
  bool trigger_result = false;
  REQUIRE(socket.asyncSend(makeEndpoint(), packet, 1, [&callback_thread](std::unique_ptr<cudp::network::UDPPacket>, asio::error_code) {
    callback_thread = std::this_thread::get_id();
  }));

  std::thread worker([&]() {
    trigger_thread = std::this_thread::get_id();
    trigger_result = socket.triggerSend();
  });
  worker.join();

  REQUIRE(trigger_result);
  REQUIRE(callback_thread == trigger_thread);
  REQUIRE(callback_thread != std::this_thread::get_id());
}

TEST_CASE("MockUDPSocket send callback may enqueue another send", "[MockUDPSocket][send][reentrant]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> first_packet  = makePacket();
  std::unique_ptr<cudp::network::UDPPacket> second_packet = makePacket();
  bool second_send_accepted                               = false;
  REQUIRE(socket.asyncSend(makeEndpoint(), first_packet, 1, [&](std::unique_ptr<cudp::network::UDPPacket>, asio::error_code) {
    second_send_accepted = socket.asyncSend(makeEndpoint(), second_packet, 1, {});
  }));

  REQUIRE(socket.triggerSend());
  REQUIRE(second_send_accepted);
  REQUIRE(second_packet == nullptr);
  REQUIRE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket catches a standard send callback exception", "[MockUDPSocket][send][exception]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  REQUIRE(socket.asyncSend(makeEndpoint(), packet, 1,
                           [](std::unique_ptr<cudp::network::UDPPacket>, asio::error_code) { throw std::runtime_error("send failure"); }));

  REQUIRE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket catches an unknown send callback exception", "[MockUDPSocket][send][exception]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  REQUIRE(socket.asyncSend(makeEndpoint(), packet, 1, [](std::unique_ptr<cudp::network::UDPPacket>, asio::error_code) { throw 42; }));

  REQUIRE(socket.triggerSend());
}

TEST_CASE("MockUDPSocket rejects a receive with a null packet", "[MockUDPSocket][receive][validation]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet;

  REQUIRE_FALSE(socket.asyncReceive(packet, {}));
  REQUIRE(packet == nullptr);
}

TEST_CASE("MockUDPSocket rejects a second pending receive without taking its packet", "[MockUDPSocket][receive][pending]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> first_packet  = makePacket();
  std::unique_ptr<cudp::network::UDPPacket> second_packet = makePacket();
  cudp::network::UDPPacket *second_packet_address         = second_packet.get();
  REQUIRE(socket.asyncReceive(first_packet, {}));

  REQUIRE_FALSE(socket.asyncReceive(second_packet, {}));
  REQUIRE(second_packet.get() == second_packet_address);
  REQUIRE(socket.triggerReceive(0));
}

TEST_CASE("MockUDPSocket receive returns packet size source and error", "[MockUDPSocket][receive][callback]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket(42);
  cudp::network::UDPPacket *packet_address         = packet.get();
  asio::ip::udp::endpoint expected_source          = makeEndpoint(8080);
  asio::error_code expected_error                  = asio::error::message_size;
  bool callback_called                             = false;
  REQUIRE(socket.asyncReceive(
      packet, [&](std::unique_ptr<cudp::network::UDPPacket> p_packet, uint32_t p_size, asio::ip::udp::endpoint p_source, asio::error_code p_error) {
        callback_called = true;
        CHECK(p_packet.get() == packet_address);
        CHECK(p_packet->m_packet_data[0] == 42);
        CHECK(p_size == 17);
        CHECK(p_source == expected_source);
        CHECK(p_error == expected_error);
      }));

  REQUIRE(packet == nullptr);
  REQUIRE_FALSE(callback_called);
  REQUIRE(socket.triggerReceive(17, expected_source, expected_error));
  REQUIRE(callback_called);
}

TEST_CASE("MockUDPSocket triggerReceive returns false without a pending receive", "[MockUDPSocket][receive][trigger]") {
  cudp::network::MockUDPSocket socket;

  REQUIRE_FALSE(socket.triggerReceive(0));
}

TEST_CASE("MockUDPSocket oversized receive trigger keeps the operation pending", "[MockUDPSocket][receive][validation]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  bool callback_called                             = false;
  REQUIRE(socket.asyncReceive(packet, [&callback_called](std::unique_ptr<cudp::network::UDPPacket>, uint32_t, asio::ip::udp::endpoint,
                                                         asio::error_code) { callback_called = true; }));

  REQUIRE_FALSE(socket.triggerReceive(cudp::network::UDPPacket::MAX_UDP_PKT_SIZE + 1));
  REQUIRE_FALSE(callback_called);
  REQUIRE(socket.triggerReceive(cudp::network::UDPPacket::MAX_UDP_PKT_SIZE));
  REQUIRE(callback_called);
}

TEST_CASE("MockUDPSocket triggerReceive accepts an empty callback", "[MockUDPSocket][receive][callback]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  REQUIRE(socket.asyncReceive(packet, {}));

  REQUIRE(socket.triggerReceive(0));
  REQUIRE_FALSE(socket.triggerReceive(0));
}

TEST_CASE("MockUDPSocket receive callback runs on the trigger thread", "[MockUDPSocket][receive][thread]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  std::thread::id callback_thread;
  std::thread::id trigger_thread;
  bool trigger_result = false;
  REQUIRE(socket.asyncReceive(packet, [&callback_thread](std::unique_ptr<cudp::network::UDPPacket>, uint32_t, asio::ip::udp::endpoint,
                                                         asio::error_code) { callback_thread = std::this_thread::get_id(); }));

  std::thread worker([&]() {
    trigger_thread = std::this_thread::get_id();
    trigger_result = socket.triggerReceive(0);
  });
  worker.join();

  REQUIRE(trigger_result);
  REQUIRE(callback_thread == trigger_thread);
  REQUIRE(callback_thread != std::this_thread::get_id());
}

TEST_CASE("MockUDPSocket receive callback may enqueue another receive", "[MockUDPSocket][receive][reentrant]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> first_packet  = makePacket();
  std::unique_ptr<cudp::network::UDPPacket> second_packet = makePacket();
  bool second_receive_accepted                            = false;
  REQUIRE(socket.asyncReceive(first_packet, [&](std::unique_ptr<cudp::network::UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code) {
    second_receive_accepted = socket.asyncReceive(second_packet, {});
  }));

  REQUIRE(socket.triggerReceive(0));
  REQUIRE(second_receive_accepted);
  REQUIRE(second_packet == nullptr);
  REQUIRE(socket.triggerReceive(0));
}

TEST_CASE("MockUDPSocket catches a standard receive callback exception", "[MockUDPSocket][receive][exception]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  REQUIRE(socket.asyncReceive(packet, [](std::unique_ptr<cudp::network::UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code) {
    throw std::runtime_error("receive failure");
  }));

  REQUIRE(socket.triggerReceive(0));
}

TEST_CASE("MockUDPSocket catches an unknown receive callback exception", "[MockUDPSocket][receive][exception]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> packet = makePacket();
  REQUIRE(
      socket.asyncReceive(packet, [](std::unique_ptr<cudp::network::UDPPacket>, uint32_t, asio::ip::udp::endpoint, asio::error_code) { throw 42; }));

  REQUIRE(socket.triggerReceive(0));
}

TEST_CASE("MockUDPSocket allows one send and one receive to be pending together", "[MockUDPSocket][send][receive][pending]") {
  cudp::network::MockUDPSocket socket;
  std::unique_ptr<cudp::network::UDPPacket> send_packet    = makePacket();
  std::unique_ptr<cudp::network::UDPPacket> receive_packet = makePacket();

  REQUIRE(socket.asyncSend(makeEndpoint(), send_packet, 1, {}));
  REQUIRE(socket.asyncReceive(receive_packet, {}));
  REQUIRE(socket.triggerSend());
  REQUIRE(socket.triggerReceive(0));
}
