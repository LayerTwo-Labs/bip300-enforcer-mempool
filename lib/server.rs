use async_trait::async_trait;
use bip300301::client::{
    BlockTemplate, BlockTemplateRequest, BlockTemplateTransaction,
    CoinbaseTxnOrValue, NetworkInfo,
};
use bitcoin::{amount::CheckedSum, Transaction};
use chrono::Utc;
use jsonrpsee::{core::RpcResult, proc_macros::rpc, types::ErrorCode};
use thiserror::Error;

use crate::{cusf_enforcer::CusfEnforcer, mempool::MempoolSync};

#[rpc(server)]
pub trait Rpc {
    #[method(name = "getblocktemplate")]
    async fn get_block_template(
        &self,
        _request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate>;
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("Sample block template cannot set coinbasetxn field")]
    SampleBlockTemplate,
}

pub struct Server<Enforcer> {
    mempool: MempoolSync<Enforcer>,
    network_info: NetworkInfo,
    sample_block_template: BlockTemplate,
}

impl<Enforcer> Server<Enforcer> {
    pub fn new(
        mempool: MempoolSync<Enforcer>,
        network_info: NetworkInfo,
        sample_block_template: BlockTemplate,
    ) -> Result<Self, CreateServerError> {
        if matches!(
            sample_block_template.coinbase_txn_or_value,
            CoinbaseTxnOrValue::Txn(_)
        ) {
            return Err(CreateServerError::SampleBlockTemplate);
        };
        Ok(Self {
            mempool,
            network_info,
            sample_block_template,
        })
    }
}

fn log_error<Err>(err: Err) -> anyhow::Error
where
    anyhow::Error: From<Err>,
{
    let err = anyhow::Error::from(err);
    tracing::error!("{err:#}");
    err
}

fn internal_error<Err>(err: Err) -> jsonrpsee::types::ErrorObjectOwned
where
    anyhow::Error: From<Err>,
{
    let err = anyhow::Error::from(err);
    let err_msg = format!("{err:#}");
    jsonrpsee::types::ErrorObjectOwned::owned(
        ErrorCode::InternalError.code(),
        ErrorCode::InternalError.message(),
        Some(err_msg),
    )
}

#[async_trait]
impl<Enforcer> RpcServer for Server<Enforcer>
where
    Enforcer: CusfEnforcer + Send + Sync + 'static,
{
    async fn get_block_template(
        &self,
        request: BlockTemplateRequest,
    ) -> RpcResult<BlockTemplate> {
        const NONCE_RANGE: [u8; 8] = [0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF];

        let now = Utc::now();
        let BlockTemplate {
            version,
            ref rules,
            ref version_bits_available,
            version_bits_required,
            ref coinbase_aux,
            // FIXME: compute this
            ref coinbase_txn_or_value,
            ref mutable,
            sigop_limit,
            size_limit,
            weight_limit,
            ..
        } = self.sample_block_template;

        // Some(_) iff client has indicated support for coinbasetxn
        let mut coinbase_txn: Option<Transaction> =
            if request.capabilities.contains("coinbasetxn") {
                Some(Transaction {
                    version: bitcoin::transaction::Version::TWO,
                    lock_time: bitcoin::absolute::LockTime::ZERO,
                    input: Vec::new(),
                    output: Vec::new(),
                })
            } else {
                None
            };
        let (
            target,
            prev_blockhash,
            tip_block_mediantime,
            tip_block_height,
            transactions,
        ) = self
            .mempool
            .with(|mempool, enforcer| {
                let tip_block = mempool.tip();
                if let Some(coinbase_txn) = coinbase_txn.as_mut() {
                    coinbase_txn.output = enforcer
                        .modify_coinbase(Vec::new())
                        .map_err(either::Either::Left)?
                };
                Ok((
                    mempool.next_target(),
                    tip_block.hash,
                    tip_block.mediantime,
                    tip_block.height,
                    mempool.propose_txs().map_err(either::Either::Right)?,
                ))
            })
            .await
            .map_err(|err: either::Either<_, _>| {
                let err = log_error(err);
                internal_error(err)
            })?;
        let coinbase_txn_or_value = if let Some(coinbase_txn) = coinbase_txn {
            let fee = coinbase_txn
                .output
                .iter()
                .map(|txout| txout.value)
                .checked_sum()
                .ok_or_else(|| {
                    let err = anyhow::anyhow!(
                        "Value overflow error in coinbase output"
                    );
                    let err = log_error(err);
                    internal_error(err)
                })?;
            let txn = BlockTemplateTransaction {
                txid: coinbase_txn.compute_txid(),
                hash: coinbase_txn.compute_wtxid(),
                depends: Vec::new(),
                fee: -(fee.to_sat() as i64),
                sigops: Some(coinbase_txn.total_sigop_cost(|_| None) as u64),
                weight: coinbase_txn.weight().to_wu(),
                data: bitcoin::consensus::serialize(&coinbase_txn),
            };
            CoinbaseTxnOrValue::Txn(txn)
        } else {
            coinbase_txn_or_value.clone()
        };
        let current_time_adjusted =
            (now.timestamp() + self.network_info.time_offset_s) as u64;
        let mintime = std::cmp::max(
            tip_block_mediantime as u64 + 1,
            current_time_adjusted,
        );
        let height = tip_block_height as u32 + 1;
        let res = BlockTemplate {
            signet_challenge: self
                .sample_block_template
                .signet_challenge
                .clone(),
            version,
            rules: rules.clone(),
            version_bits_available: version_bits_available.clone(),
            version_bits_required,
            prev_blockhash,
            transactions,
            coinbase_aux: coinbase_aux.clone(),
            coinbase_txn_or_value,
            long_poll_id: None,
            target: target.to_le_bytes(),
            mintime,
            mutable: mutable.clone(),
            nonce_range: NONCE_RANGE,
            sigop_limit,
            size_limit,
            weight_limit,
            current_time: current_time_adjusted,
            compact_target: target.to_compact_lossy(),
            height,
            // FIXME: set this
            default_witness_commitment: None,
        };
        Ok(res)
    }
}
