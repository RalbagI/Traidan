// ZeroMQ Push/Pull and Pub/Sub socket management.
// Push/Pull: Python → Rust trade commands (sub-20 µs).
// Pub/Sub:   Rust → Python portfolio state, VaR warnings, fill reports.
// See architecture.md §5 — ZeroMQ IPC.

pub const COMMAND_ENDPOINT: &str = "tcp://127.0.0.1:5555";
pub const BROADCAST_ENDPOINT: &str = "tcp://127.0.0.1:5556";

/// Start the Pull socket; blocks on incoming signals from the Python layer.
pub fn start_command_receiver() {
    // TODO: initialize zmq::Context, bind PULL socket to COMMAND_ENDPOINT,
    //       deserialize incoming trade signal frames, push onto ring buffer
}

/// Publish a portfolio state snapshot to all Python subscribers.
pub fn publish_portfolio_state(_state_json: &str) {
    // TODO: initialize PUB socket bound to BROADCAST_ENDPOINT, send frames
}
