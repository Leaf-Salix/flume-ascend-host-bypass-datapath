#include "test_util.h"

#include <sys/socket.h>
#include <unistd.h>

#include <string>
#include <thread>

#include "protocol/framing.h"

int main() {
  int fds[2] = {-1, -1};
  FLUME_TEST_CHECK(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

  flume::protocol::Frame sent;
  sent.type = flume::protocol::FrameType::kReadReq;
  sent.request_id = 42;
  flume::protocol::AppendU64(&sent.body, 7);
  flume::protocol::AppendString(&sent.body, "hello");

  flume::protocol::Frame got;
  std::string write_error;
  std::string read_error;

  std::thread writer([&] {
    FLUME_TEST_CHECK(flume::protocol::WriteFrame(fds[0], sent, &write_error));
  });
  FLUME_TEST_CHECK(flume::protocol::ReadFrame(fds[1], &got, &read_error));
  writer.join();
  FLUME_TEST_CHECK(got.type == sent.type);
  FLUME_TEST_CHECK(got.request_id == sent.request_id);

  flume::protocol::Reader reader(got.body);
  uint64_t id = 0;
  std::string text;
  FLUME_TEST_CHECK(reader.ReadU64(&id));
  FLUME_TEST_CHECK(reader.ReadString(&text));
  FLUME_TEST_CHECK(reader.Done());
  FLUME_TEST_CHECK(id == 7);
  FLUME_TEST_CHECK(text == "hello");

  close(fds[0]);
  close(fds[1]);
  return 0;
}
