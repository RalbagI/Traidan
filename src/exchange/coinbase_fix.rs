// Coinbase FIX 5.0 order entry gateway.
// Persistent TCP+SSL session to fix-ord.exchange.coinbase.com:6121.
// Manages sequence numbers, Logon handshake, heartbeats, and NewOrderSingle.
// See architecture.md §4 — Coinbase FIX.

use uuid::Uuid;

pub struct FixSession {
    pub seq_num: u64,
    pub sender_comp_id: String,
    pub target_comp_id: String,
}

impl FixSession {
    pub fn new(sender: impl Into<String>, target: impl Into<String>) -> Self {
        Self {
            seq_num: 1,
            sender_comp_id: sender.into(),
            target_comp_id: target.into(),
        }
    }

    /// Build a NewOrderSingle (35=D) FIX message string.
    pub fn new_order_single(
        &mut self,
        symbol: &str,
        side: char,
        qty: f64,
        price: Option<f64>,
        time_in_force: char,
    ) -> String {
        let cl_ord_id = Uuid::new_v4().to_string();
        let ord_type = if price.is_some() { '2' } else { '1' };
        let mut msg = format!(
            "8=FIXT.1.1\x0135=D\x0134={}\x0149={}\x0156={}\x0111={}\x0155={}\x0154={}\x0140={}\x0159={}\x0138={:.8}\x01",
            self.seq_num,
            self.sender_comp_id,
            self.target_comp_id,
            cl_ord_id,
            symbol,
            side,
            ord_type,
            time_in_force,
            qty,
        );
        if let Some(p) = price {
            msg.push_str(&format!("44={:.8}\x01", p));
        }
        self.seq_num += 1;
        msg
    }
}
