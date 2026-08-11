#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace mooncake {
namespace hkvm {

// Description of one upstream Mooncake master whose KV events are consumed.
struct DiffMasterConfig {
  // Logical name used in reports, e.g. "master1".
  std::string id;
  // ZMQ endpoint the master's KvEventPublisher binds to,
  // e.g. "tcp://10.0.0.1:5557" or "ipc:///tmp/kv_events".
  std::string endpoint;
  // If non-empty, only events whose "backend_id" field matches are counted;
  // empty accepts every event arriving on this socket.
  std::string backend_id_filter;
};

struct DiffServerConfig {
  // Two masters are expected for the two-list diff. masters_[0] is "master1"
  // and masters_[1] is "master2" in DiffResult.
  std::vector<DiffMasterConfig> masters;
  // ZMQ SUB receive high-water mark per socket.
  int recv_hwm{100000};
  // Receiver poll timeout in milliseconds (also caps stop latency).
  int poll_timeout_ms{200};
};

// Consumes KV events (Mooncake KvEventPublisher wire format, RFC #1527) from
// two Mooncake masters over ZMQ and maintains the live object-key set of each.
//
// The symmetric difference is exposed as two lists:
//   only_in_master1 : keys present in master1 but absent from master2
//   only_in_master2 : keys present in master2 but absent from master1
//
// Design note: the two diff lists are derived on demand from the per-master
// live key sets rather than maintained incrementally. An event stream is
// inherently racy across two independent masters (a "stored" for key K may
// arrive from master1 before/after the corresponding event from master2), so
// the live sets are the source of truth and GetDiff() produces a consistent
// snapshot under both locks.
class DiffServer {
 public:
  explicit DiffServer(DiffServerConfig config);
  ~DiffServer();

  DiffServer(const DiffServer&) = delete;
  DiffServer& operator=(const DiffServer&) = delete;

  // Connects SUB sockets and starts one receiver thread per master.
  // Returns false if fewer than two masters are configured or ZMQ setup fails.
  bool Start();
  // Signals receiver threads to stop, joins them, and tears down ZMQ state.
  void Stop();

  bool running() const { return running_.load(); }

  struct DiffResult {
    std::vector<std::string> only_in_master1;
    std::vector<std::string> only_in_master2;
    size_t master1_key_count{0};
    size_t master2_key_count{0};
  };
  // Returns the current symmetric difference, sorted for stable output.
  DiffResult GetDiff() const;

  struct MasterStats {
    uint64_t events_received{0};  // events parsed and applied (post-filter)
    uint64_t stored_events{0};
    uint64_t removed_events{0};
    uint64_t malformed_events{0};
    uint64_t seq_gaps{0};  // publisher-side drops detected via ZMQ seq gaps
    uint64_t last_seq{0};  // last ZMQ sequence seen; 0 == none seen
    bool has_last_seq{false};
  };
  std::vector<MasterStats> GetStats() const;

 private:
  struct MasterState {
    DiffMasterConfig config;

    mutable std::mutex mutex;
    std::unordered_set<std::string> live_keys;

    // Per-socket monotonic ZMQ sequence tracking. Updated only by the receiver
    // thread, but read by GetStats(), so they are atomic.
    std::atomic<bool> has_last_seq{false};
    std::atomic<uint64_t> last_seq{0};

    std::atomic<uint64_t> events_received{0};
    std::atomic<uint64_t> stored_events{0};
    std::atomic<uint64_t> removed_events{0};
    std::atomic<uint64_t> malformed_events{0};
    std::atomic<uint64_t> seq_gaps{0};

    // Owned exclusively by the receiver thread.
    void* socket{nullptr};
    std::thread thread;
    std::atomic<bool> stop_flag{false};
  };

  void ReceiverLoop(size_t index);
  bool ApplyEvent(MasterState& state, const void* payload, size_t size);
  void CloseSockets();

  DiffServerConfig config_;
  void* zmq_context_{nullptr};
  std::vector<std::unique_ptr<MasterState>> masters_;
  std::atomic<bool> running_{false};
};

}  // namespace hkvm
}  // namespace mooncake
