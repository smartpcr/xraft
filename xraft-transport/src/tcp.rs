//! TCP transport implementation for production use.
//!
//! Provides `TcpTransport` which implements the split transport pattern:
//! `split()` returns `(Box<dyn TransportSender>, Box<dyn TransportReceiver>)`.
//!
//! - `TcpSender`: connection-pooled outbound RPCs with exponential backoff reconnect.
//!   Uses a per-peer background worker so `send()` never holds a mutex across I/O;
//!   concurrent sends to different peers proceed in parallel and sends to the same
//!   peer are naturally serialised by the worker's channel.
//! - `TcpReceiver`: accepts inbound connections and decodes framed messages.
//!   On drop, immediately shuts down every accepted socket (via a cloned std handle)
//!   so the sender side receives FIN/RST synchronously — no reliance on async task abort.
//!
//! All messages use length-prefixed framing via `tokio_util::codec::LengthDelimitedCodec`
//! and bincode serialisation.

use std::collections::HashMap;
use std::net::{Shutdown, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::SinkExt;
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, warn};

use xraft_core::error::XraftError;
use xraft_core::rpc::RpcEnvelope;
use xraft_core::traits::{TransportReceiver, TransportSender};
use xraft_core::types::NodeId;
use xraft_core::Result;

/// Maximum frame size for length-delimited codec (16 MiB).
const MAX_FRAME_LENGTH: usize = 16 * 1024 * 1024;

/// Default base delay for exponential backoff reconnection.
const DEFAULT_BACKOFF_BASE: Duration = Duration::from_millis(100);

/// Default maximum backoff delay.
const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Default maximum number of reconnection attempts before failing a send.
const DEFAULT_MAX_RETRIES: u32 = 5;

/// Configuration for TCP transport backoff and retry behavior.
#[derive(Debug, Clone)]
pub struct TcpTransportConfig {
    pub backoff_base: Duration,
    pub backoff_max: Duration,
    pub max_retries: u32,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        Self {
            backoff_base: DEFAULT_BACKOFF_BASE,
            backoff_max: DEFAULT_BACKOFF_MAX,
            max_retries: DEFAULT_MAX_RETRIES,
        }
    }
}

/// Production TCP transport.
///
/// Binds a `TcpListener` for inbound connections and maintains a pool of
/// outbound connections to peers. Call `split()` to obtain the sender and
/// receiver halves.
pub struct TcpTransport {
    listener: TcpListener,
    peers: HashMap<NodeId, SocketAddr>,
    config: TcpTransportConfig,
}

impl TcpTransport {
    /// Create a new TCP transport bound to `bind_addr`.
    ///
    /// `peers` maps each remote node's `NodeId` to its listening `SocketAddr`.
    pub async fn new(
        bind_addr: SocketAddr,
        peers: HashMap<NodeId, SocketAddr>,
    ) -> std::io::Result<Self> {
        let listener = Self::bind_listener(bind_addr).await?;
        Ok(Self {
            listener,
            peers,
            config: TcpTransportConfig::default(),
        })
    }

    /// Create a new TCP transport with custom retry/backoff configuration.
    pub async fn with_config(
        bind_addr: SocketAddr,
        peers: HashMap<NodeId, SocketAddr>,
        config: TcpTransportConfig,
    ) -> std::io::Result<Self> {
        let listener = Self::bind_listener(bind_addr).await?;
        Ok(Self {
            listener,
            peers,
            config,
        })
    }

    /// Bind a TCP listener with `SO_REUSEADDR` enabled so servers can restart
    /// quickly without hitting `EADDRINUSE` from sockets lingering in `TIME_WAIT`.
    async fn bind_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
        let socket = if addr.is_ipv4() {
            tokio::net::TcpSocket::new_v4()?
        } else {
            tokio::net::TcpSocket::new_v6()?
        };
        socket.set_reuseaddr(true)?;
        socket.bind(addr)?;
        socket.listen(1024)
    }

    /// Split into sender and receiver halves per architecture §4.4.
    pub fn split(self) -> (Box<dyn TransportSender>, Box<dyn TransportReceiver>) {
        let (envelope_tx, envelope_rx) = mpsc::channel::<RpcEnvelope>(1024);

        // Spawn one background worker per peer. Each worker owns its connection
        // exclusively — no mutex needed. `send()` enqueues a request on the
        // per-peer channel; the worker handles connect / write / reconnect.
        let mut channels: HashMap<NodeId, mpsc::Sender<SendRequest>> = HashMap::new();
        let mut worker_handles: Vec<JoinHandle<()>> = Vec::new();

        for (&node_id, &addr) in &self.peers {
            let (tx, rx) = mpsc::channel::<SendRequest>(256);
            let handle = tokio::spawn(sender_worker(node_id, addr, rx, self.config.clone()));
            channels.insert(node_id, tx);
            worker_handles.push(handle);
        }

        let sender = TcpSender {
            channels: Arc::new(channels),
            _worker_handles: Arc::new(worker_handles),
        };

        // Shared registries for accepted socket cleanup.
        let reader_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let accepted_sockets: Arc<std::sync::Mutex<Vec<std::net::TcpStream>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let accept_handle = tokio::spawn(accept_loop(
            self.listener,
            envelope_tx,
            Arc::clone(&reader_handles),
            Arc::clone(&accepted_sockets),
        ));

        let receiver = TcpReceiver {
            rx: envelope_rx,
            accept_handle,
            reader_handles,
            accepted_sockets,
        };

        (Box::new(sender), Box::new(receiver))
    }
}

// ---------------------------------------------------------------------------
// Sender
// ---------------------------------------------------------------------------

/// A request to send data to a peer, with a oneshot for the result.
struct SendRequest {
    data: Bytes,
    reply: oneshot::Sender<Result<()>>,
}

/// Outbound TCP sender — no mutexes, no blocking.
///
/// Each peer has a dedicated background worker task that owns the connection.
/// `send()` enqueues a `SendRequest` on the per-peer channel and awaits the
/// reply. Concurrent sends to different peers proceed in parallel (different
/// channels). Concurrent sends to the same peer are naturally serialised by
/// the channel, which is correct because a single TCP connection requires
/// serialised writes.
pub struct TcpSender {
    channels: Arc<HashMap<NodeId, mpsc::Sender<SendRequest>>>,
    // Keep worker handles alive; they exit when the channel closes (sender dropped).
    _worker_handles: Arc<Vec<JoinHandle<()>>>,
}

fn new_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec()
}

/// Per-peer background task. Owns one TCP connection, handles reconnect with
/// exponential backoff.
async fn sender_worker(
    peer_id: NodeId,
    addr: SocketAddr,
    mut rx: mpsc::Receiver<SendRequest>,
    config: TcpTransportConfig,
) {
    let mut conn: Option<Framed<TcpStream, LengthDelimitedCodec>> = None;

    while let Some(req) = rx.recv().await {
        let result =
            send_with_retry(&mut conn, peer_id, addr, req.data, &config).await;
        // If the caller dropped the oneshot, we just discard the result.
        let _ = req.reply.send(result);
    }
}

/// Try to send `data` to `addr`, reconnecting with exponential backoff on failure.
async fn send_with_retry(
    conn: &mut Option<Framed<TcpStream, LengthDelimitedCodec>>,
    peer_id: NodeId,
    addr: SocketAddr,
    data: Bytes,
    config: &TcpTransportConfig,
) -> Result<()> {
    let mut backoff = config.backoff_base;

    for attempt in 0..=config.max_retries {
        let result = try_send_once(conn, peer_id, addr, data.clone()).await;
        match result {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt == config.max_retries {
                    return Err(XraftError::TransportError(format!(
                        "failed to send to {peer_id:?} after {} attempts: {e}",
                        config.max_retries + 1
                    )));
                }
                debug!(
                    ?peer_id,
                    attempt,
                    backoff_ms = backoff.as_millis(),
                    "send failed, retrying: {e}"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(config.backoff_max);
            }
        }
    }
    unreachable!()
}

/// Single send attempt. Probes liveness, (re)connects if needed, writes frame.
async fn try_send_once(
    conn: &mut Option<Framed<TcpStream, LengthDelimitedCodec>>,
    peer_id: NodeId,
    addr: SocketAddr,
    data: Bytes,
) -> Result<()> {
    // Probe existing connection with an async peek to reliably detect FIN/RST.
    if let Some(ref framed) = conn {
        if !probe_alive(framed.get_ref()).await {
            debug!(?peer_id, "stale connection detected via probe, dropping");
            *conn = None;
        }
    }

    // Establish a new connection if needed.
    if conn.is_none() {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true).ok();
        *conn = Some(Framed::new(stream, new_codec()));
    }

    let framed = conn.as_mut().unwrap();
    if let Err(e) = framed.send(data).await {
        *conn = None;
        return Err(XraftError::TransportError(format!(
            "write to {peer_id:?} failed: {e}"
        )));
    }

    // Post-write probe: SinkExt::send flushes, but the kernel may have buffered
    // the write to a half-closed socket. A brief async peek detects the FIN/RST
    // that arrives after the peer acknowledged our data with RST.
    if !probe_alive(framed.get_ref()).await {
        *conn = None;
        return Err(XraftError::TransportError(format!(
            "connection to {peer_id:?} closed by peer after send"
        )));
    }

    Ok(())
}

/// Async liveness probe. Waits briefly (up to 5 ms) for incoming FIN/RST via
/// `peek`. Unlike a non-blocking `try_read`, this gives the tokio runtime a
/// chance to poll the socket and surface pending close events.
///
/// Returns `false` if the connection received EOF or an error.
async fn probe_alive(stream: &TcpStream) -> bool {
    let mut buf = [0u8; 1];
    tokio::select! {
        biased;
        result = stream.peek(&mut buf) => {
            match result {
                Ok(0) => false,   // EOF — peer closed
                Err(_) => false,  // RST or other error
                Ok(_) => true,    // data pending (shouldn't happen, but alive)
            }
        }
        _ = tokio::time::sleep(Duration::from_millis(5)) => {
            true // no FIN/RST within probe window — treat as alive
        }
    }
}

#[async_trait]
impl TransportSender for TcpSender {
    async fn send(&self, target: NodeId, message: RpcEnvelope) -> Result<()> {
        let tx = self
            .channels
            .get(&target)
            .ok_or_else(|| XraftError::TransportError(format!("unknown peer: {target:?}")))?;

        let encoded = bincode::serialize(&message)
            .map_err(|e| XraftError::SerializationError(e.to_string()))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(SendRequest {
            data: Bytes::from(encoded),
            reply: reply_tx,
        })
        .await
        .map_err(|_| XraftError::TransportError("sender worker gone".into()))?;

        reply_rx
            .await
            .map_err(|_| XraftError::TransportError("sender worker dropped reply".into()))?
    }
}

// ---------------------------------------------------------------------------
// Receiver
// ---------------------------------------------------------------------------

/// Inbound TCP receiver.
///
/// Backed by an internal `mpsc` channel fed by an accept loop that spawns
/// per-connection read tasks.
///
/// On drop, every accepted socket is **synchronously** shut down via a cloned
/// `std::net::TcpStream` handle. This sends FIN/RST to the sender side
/// immediately — we do not rely on async task abort propagation, which can
/// race with subsequent sends.
pub struct TcpReceiver {
    rx: mpsc::Receiver<RpcEnvelope>,
    accept_handle: JoinHandle<()>,
    reader_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
    /// Cloned std handles for every accepted connection. `shutdown(Both)` on
    /// these is synchronous and affects the underlying socket even though the
    /// tokio `TcpStream` in the reader task still holds the original FD.
    accepted_sockets: Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
}

impl Drop for TcpReceiver {
    fn drop(&mut self) {
        // 1. Abort the accept loop so no new connections are accepted.
        self.accept_handle.abort();

        // 2. Synchronously shutdown every accepted socket. This sends FIN to
        //    the sender side immediately, regardless of whether the reader
        //    task has been polled/aborted yet.
        {
            let mut sockets = self.accepted_sockets.lock().unwrap();
            for socket in sockets.drain(..) {
                let _ = socket.shutdown(Shutdown::Both);
            }
        }

        // 3. Abort reader tasks to clean up resources.
        {
            let handles = self.reader_handles.lock().unwrap();
            for handle in handles.iter() {
                handle.abort();
            }
        }
    }
}

#[async_trait]
impl TransportReceiver for TcpReceiver {
    async fn recv(&mut self) -> Result<RpcEnvelope> {
        self.rx
            .recv()
            .await
            .ok_or_else(|| XraftError::TransportError("receiver channel closed".into()))
    }
}

/// Accept loop: listens for inbound TCP connections and spawns a reader task
/// for each. Clones each accepted socket's std handle into `accepted_sockets`
/// so `TcpReceiver::drop` can shut them down synchronously.
async fn accept_loop(
    listener: TcpListener,
    tx: mpsc::Sender<RpcEnvelope>,
    reader_handles: Arc<std::sync::Mutex<Vec<JoinHandle<()>>>>,
    accepted_sockets: Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer_addr)) => {
                        debug!(%peer_addr, "accepted inbound connection");
                        stream.set_nodelay(true).ok();

                        // Convert tokio→std→clone→back so we keep a std handle
                        // for synchronous shutdown in TcpReceiver::drop.
                        let stream = match clone_accepted_socket(stream, &accepted_sockets) {
                            Ok(s) => s,
                            Err(e) => {
                                warn!(%peer_addr, "failed to clone accepted socket: {e}");
                                continue;
                            }
                        };

                        let framed = Framed::new(stream, new_codec());
                        let tx = tx.clone();
                        let handle = tokio::spawn(connection_reader(framed, tx, peer_addr));
                        reader_handles.lock().unwrap().push(handle);
                    }
                    Err(e) => {
                        warn!("accept error: {e}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            _ = tx.closed() => {
                debug!("envelope channel closed, stopping accept loop");
                return;
            }
        }
    }
}

/// Clone the accepted tokio `TcpStream`'s underlying socket handle via
/// `into_std` → `try_clone` → `from_std`. The clone is stored for
/// synchronous `shutdown(Both)` in `TcpReceiver::drop`; the original is
/// re-registered with the tokio reactor and returned.
fn clone_accepted_socket(
    stream: TcpStream,
    store: &Arc<std::sync::Mutex<Vec<std::net::TcpStream>>>,
) -> std::io::Result<TcpStream> {
    let std_stream = stream.into_std()?;
    let std_clone = std_stream.try_clone()?;
    let tokio_stream = TcpStream::from_std(std_stream)?;
    store.lock().unwrap().push(std_clone);
    Ok(tokio_stream)
}

/// Read loop for a single inbound connection. Decodes framed messages and
/// forwards them into the envelope channel.
async fn connection_reader(
    mut framed: Framed<TcpStream, LengthDelimitedCodec>,
    tx: mpsc::Sender<RpcEnvelope>,
    peer_addr: SocketAddr,
) {
    while let Some(frame_result) = framed.next().await {
        match frame_result {
            Ok(frame) => {
                let data: BytesMut = frame;
                match bincode::deserialize::<RpcEnvelope>(&data) {
                    Ok(envelope) => {
                        if tx.send(envelope).await.is_err() {
                            debug!(%peer_addr, "envelope channel closed, stopping reader");
                            return;
                        }
                    }
                    Err(e) => {
                        warn!(%peer_addr, "deserialization error: {e}");
                    }
                }
            }
            Err(e) => {
                debug!(%peer_addr, "connection read error: {e}");
                return;
            }
        }
    }
    debug!(%peer_addr, "connection closed");
}

#[cfg(test)]
mod tests {
    use super::*;
    use xraft_core::rpc::{FetchRequest, RpcPayload};
    use xraft_core::types::{ClusterId, Term};

    fn test_cluster_id() -> ClusterId {
        ClusterId(uuid::Uuid::new_v4())
    }

    fn make_fetch_envelope(cluster_id: ClusterId, source: NodeId) -> RpcEnvelope {
        RpcEnvelope {
            cluster_id,
            leader_epoch: Term(1),
            source,
            payload: RpcPayload::FetchRequest(FetchRequest {
                replica_id: source,
                fetch_offset: 42,
                last_fetched_epoch: Term(1),
                max_bytes: 1024,
            }),
        }
    }

    /// TCP roundtrip: node A sends a FetchRequest to node B, B receives it correctly.
    #[tokio::test]
    async fn tcp_roundtrip() {
        let cluster_id = test_cluster_id();
        let node_a = NodeId(1);
        let node_b = NodeId(2);

        // Bind B first to get its address.
        let addr_b: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport_b = TcpTransport::new(addr_b, HashMap::new()).await.unwrap();
        let actual_addr_b = transport_b.listener.local_addr().unwrap();

        // Create A with B as a peer.
        let mut peers_a = HashMap::new();
        peers_a.insert(node_b, actual_addr_b);
        let addr_a: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport_a = TcpTransport::new(addr_a, peers_a).await.unwrap();

        let (sender_a, _receiver_a) = transport_a.split();
        let (_sender_b, mut receiver_b) = transport_b.split();

        // Send from A to B.
        let envelope = make_fetch_envelope(cluster_id, node_a);
        sender_a.send(node_b, envelope.clone()).await.unwrap();

        // Receive on B with a timeout to prevent hanging.
        let received = tokio::time::timeout(Duration::from_secs(5), receiver_b.recv())
            .await
            .expect("recv timed out")
            .unwrap();

        // Verify fields.
        assert_eq!(received.cluster_id, cluster_id);
        assert_eq!(received.source, node_a);
        assert_eq!(received.leader_epoch, Term(1));
        match &received.payload {
            RpcPayload::FetchRequest(req) => {
                assert_eq!(req.replica_id, node_a);
                assert_eq!(req.fetch_offset, 42);
                assert_eq!(req.last_fetched_epoch, Term(1));
                assert_eq!(req.max_bytes, 1024);
            }
            other => panic!("expected FetchRequest, got {other:?}"),
        }
    }

    /// Reconnection: after B's transport is dropped and restarted, A reconnects
    /// and delivers the message after backoff.
    #[tokio::test]
    async fn reconnection() {
        let cluster_id = test_cluster_id();
        let node_a = NodeId(1);
        let node_b = NodeId(2);

        // Bind B to get a port.
        let addr_b: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let transport_b = TcpTransport::new(addr_b, HashMap::new()).await.unwrap();
        let actual_addr_b = transport_b.listener.local_addr().unwrap();

        // Create A with fast backoff for testing.
        let mut peers_a = HashMap::new();
        peers_a.insert(node_b, actual_addr_b);
        let addr_a: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let config = TcpTransportConfig {
            backoff_base: Duration::from_millis(20),
            backoff_max: Duration::from_millis(200),
            max_retries: 10,
        };
        let transport_a =
            TcpTransport::with_config(addr_a, peers_a, config).await.unwrap();
        let (sender_a, _receiver_a) = transport_a.split();

        // Phase 1: establish a connection and send successfully.
        let (_sender_b1, mut receiver_b1) = transport_b.split();
        let envelope1 = make_fetch_envelope(cluster_id, node_a);
        sender_a.send(node_b, envelope1).await.unwrap();
        let msg1 = tokio::time::timeout(Duration::from_secs(5), receiver_b1.recv())
            .await
            .expect("phase 1 recv timed out")
            .unwrap();
        assert_eq!(msg1.source, node_a);

        // Phase 2: drop B's receiver (and sender). TcpReceiver::drop
        // synchronously shuts down every accepted socket, which sends
        // FIN/RST to sender_a's cached connection immediately.
        drop(_sender_b1);
        drop(receiver_b1);

        // Brief yield so the tokio runtime can process the close events.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Phase 3: restart B on the same address.
        let transport_b2 = TcpTransport::new(actual_addr_b, HashMap::new())
            .await
            .unwrap();
        let (_sender_b2, mut receiver_b2) = transport_b2.split();

        // Send from A — the sender worker detects the broken connection via
        // probe_alive(), reconnects with backoff, and delivers successfully.
        let envelope2 = make_fetch_envelope(cluster_id, node_a);
        tokio::time::timeout(Duration::from_secs(10), sender_a.send(node_b, envelope2))
            .await
            .expect("send timed out")
            .unwrap();

        // B receives the message on the new transport.
        let msg2 = tokio::time::timeout(Duration::from_secs(5), receiver_b2.recv())
            .await
            .expect("phase 3 recv timed out")
            .unwrap();
        assert_eq!(msg2.source, node_a);
        match &msg2.payload {
            RpcPayload::FetchRequest(req) => {
                assert_eq!(req.fetch_offset, 42);
            }
            other => panic!("expected FetchRequest, got {other:?}"),
        }
    }
}
