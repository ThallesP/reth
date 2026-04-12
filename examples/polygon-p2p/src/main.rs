//! Example for how to hook into the polygon p2p network
//!
//! Run with
//!
//! ```sh
//! cargo run -p example-polygon-p2p
//! ```
//!
//! This launches a regular reth node overriding the engine api payload builder with our custom.
//!
//! Credits to: <https://merkle.io/blog/modifying-reth-to-build-the-fastest-transaction-network-on-bsc-and-polygon>

#![warn(unused_crate_dependencies)]

use alloy_eips::BlockHashOrNumber;
use chain_cfg::{boot_nodes, head, polygon_chain_spec};
use eyre::{eyre, Result};
use reth_discv4::Discv4ConfigBuilder;
use reth_ethereum::{
    network::{
        api::events::{PeerEvent, SessionInfo},
        config::NetworkMode,
        eth_wire::{EthVersion, HelloMessage},
        NetworkConfig, NetworkEvent, NetworkEventListenerProvider, NetworkManager, PeersInfo,
    },
    tasks::Runtime,
};
use reth_network_p2p::{download::DownloadClient, headers::client::HeadersRequest, HeadersClient};
use reth_tracing::{
    tracing::{info, warn},
    tracing_subscriber::filter::LevelFilter,
    LayerInfo, LogFormat, RethTracer, Tracer,
};
use secp256k1::{rand, SecretKey};
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};
use tokio::time::timeout;
use tokio_stream::StreamExt;

pub mod chain_cfg;

#[tokio::main]
async fn main() -> Result<()> {
    // The ECDSA private key used to create our enode identifier.
    let secret_key = SecretKey::new(&mut rand::thread_rng());

    let _ = RethTracer::new()
        .with_stdout(LayerInfo::new(
            LogFormat::Terminal,
            LevelFilter::INFO.to_string(),
            "".to_string(),
            Some("always".to_string()),
        ))
        .init();

    // Use a non-default port so the example can run next to a local node.
    let local_addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 30304);
    let chain_spec = polygon_chain_spec();
    let genesis_hash = chain_spec.genesis_hash();

    // Bor peers consistently support eth/68. Sticking to it avoids the extra
    // eth/69 status fields while still supporting header requests.
    let builder = NetworkConfig::builder(secret_key, Runtime::test());
    let hello = HelloMessage::builder(builder.get_peer_id())
        .protocols([EthVersion::Eth68.into(), EthVersion::Eth67.into(), EthVersion::Eth66.into()])
        .build();

    let net_cfg = builder
        .set_head(head())
        .network_mode(NetworkMode::Work)
        .listener_addr(local_addr)
        .hello_message(hello)
        .build_with_noop_provider(chain_spec.clone());

    let mut discv4_cfg = Discv4ConfigBuilder::default();
    discv4_cfg.add_boot_nodes(boot_nodes()).lookup_interval(Duration::from_secs(1));
    let net_cfg = net_cfg.set_discovery_v4(discv4_cfg.build());

    info!(
        genesis_hash = ?genesis_hash,
        fork_id = ?chain_spec.fork_id(&head()),
        "Polygon chain spec info"
    );

    let net_manager = NetworkManager::eth(net_cfg).await?;
    let fetch_client = net_manager.fetch_client();
    let net_handle = net_manager.handle().clone();
    let mut events = net_handle.event_listener();

    tokio::spawn(net_manager);
    info!("Looking for Polygon peers...");

    timeout(Duration::from_secs(120), async {
        while let Some(evt) = events.next().await {
            match evt {
                NetworkEvent::ActivePeerSession { info, .. } => {
                    let SessionInfo { status, client_version, version, peer_id, .. } = info;
                    info!(
                        peers = net_handle.num_connected_peers(),
                        %peer_id,
                        ?version,
                        chain = %status.chain,
                        fork_id = ?status.forkid,
                        ?client_version,
                        "Session established with a Polygon peer"
                    );
                    return Ok(());
                }
                NetworkEvent::Peer(PeerEvent::SessionClosed { peer_id, reason }) => {
                    info!(
                        peers = net_handle.num_connected_peers(),
                        %peer_id,
                        ?reason,
                        "Session closed"
                    );
                }
                NetworkEvent::Peer(_) => {}
            }
        }
        Err(eyre!("event stream ended before a Polygon session was established"))
    })
    .await
    .map_err(|_| eyre!("timed out waiting for a Polygon devp2p peer"))??;

    for attempt in 1..=5 {
        match timeout(
            Duration::from_secs(20),
            fetch_client.get_headers(HeadersRequest::one(BlockHashOrNumber::Hash(genesis_hash))),
        )
        .await
        {
            Ok(Ok(headers)) => {
                let (peer_id, headers) = headers.split();
                if let Some(header) = headers.into_iter().next() {
                    let header_hash = header.hash_slow();
                    if header_hash != genesis_hash {
                        fetch_client.report_bad_message(peer_id);
                        warn!(
                            attempt,
                            %peer_id,
                            expected = ?genesis_hash,
                            got = ?header_hash,
                            number = header.number,
                            "Peer returned the wrong Polygon genesis header"
                        );
                        continue;
                    }

                    info!(%peer_id, "Received Polygon header response");
                    println!("requested Polygon genesis header");
                    println!("{header:#?}");
                    return Ok(());
                }

                warn!(attempt, %peer_id, "Peer returned an empty header response");
            }
            Ok(Err(err)) => warn!(attempt, ?err, "Header request failed"),
            Err(_) => warn!(attempt, "Timed out requesting the Polygon genesis header"),
        }
    }

    Err(eyre!("failed to fetch the Polygon genesis header after multiple attempts"))
}
