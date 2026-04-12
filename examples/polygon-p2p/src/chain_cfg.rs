use reth_discv4::NodeRecord;
use reth_ethereum::chainspec::{ChainSpec, Head, POLYGON};

use std::sync::Arc;

const PRAGUE_BLOCK: u64 = 73440256;

pub(crate) fn polygon_chain_spec() -> Arc<ChainSpec> {
    POLYGON.clone()
}

pub(crate) fn head() -> Head {
    // Bor peers on mainnet currently advertise the fork hash through Prague.
    Head { number: PRAGUE_BLOCK, ..Default::default() }
}

pub(crate) fn boot_nodes() -> Vec<NodeRecord> {
    polygon_chain_spec().bootnodes().expect("polygon bootnodes")
}

#[cfg(test)]
mod tests {
    use super::{head, polygon_chain_spec};
    use reth_ethereum::chainspec::{ForkHash, ForkId};

    #[test]
    fn polygon_forkid_matches_live_bor_peers() {
        let expected = ForkId { hash: ForkHash([0x22, 0xd5, 0x23, 0xb2]), next: 0 };
        assert_eq!(polygon_chain_spec().fork_id(&head()), expected);
    }
}
