use std::{
    borrow::Borrow, collections::HashMap, convert::Infallible, future::Future,
};

use bitcoin::{Transaction, TxOut, Txid};
use educe::Educe;
use either::Either;
use futures::TryFutureExt;
use thiserror::Error;

use crate::zmq::BlockHashMessage;

pub trait CusfEnforcer {
    type AcceptTxError: std::error::Error + Send + Sync + 'static;

    /// Return `true` to accept the tx, or `false` to reject it.
    /// Inputs to a tx are always available.
    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>;

    type ModifyCoinbaseError: std::error::Error + Send + Sync + 'static;

    /// Modify coinbase txouts.
    fn modify_coinbase(
        &self,
        coinbase_txouts: Vec<TxOut>,
    ) -> Result<Vec<TxOut>, Self::ModifyCoinbaseError>;

    type HandleBlockHashMessageError: std::error::Error + Send + Sync + 'static;

    /// Handle a block hash message
    fn handle_block_hash_msg(
        &mut self,
        msg: BlockHashMessage,
    ) -> impl Future<Output = Result<(), Self::HandleBlockHashMessageError>> + Send;
}

/// General purpose error for [`CusfEnforcer`]
#[derive(Educe)]
#[educe(Debug(bound(
    Enforcer::AcceptTxError: std::fmt::Debug,
    Enforcer::ModifyCoinbaseError: std::fmt::Debug,
    Enforcer::HandleBlockHashMessageError: std::fmt::Debug,
)))]
#[derive(Error)]
pub enum Error<Enforcer>
where
    Enforcer: CusfEnforcer,
{
    #[error("CUSF Enforcer: error accepting tx")]
    AcceptTx(#[source] Enforcer::AcceptTxError),
    #[error("CUSF Enforcer: error modifying coinbase")]
    ModifyCoinbase(#[source] Enforcer::ModifyCoinbaseError),
    #[error("CUSF Enforcer: error handling block hash message")]
    HandleBlockHashMessage(#[source] Enforcer::HandleBlockHashMessageError),
}

/// Compose two [`CusfEnforcer`]s, left-before-right
#[derive(Debug, Default)]
pub struct Compose<C0, C1>(C0, C1);

impl<C0, C1> CusfEnforcer for Compose<C0, C1>
where
    C0: CusfEnforcer + Send,
    C1: CusfEnforcer + Send,
{
    type AcceptTxError = Either<C0::AcceptTxError, C1::AcceptTxError>;

    fn accept_tx<TxRef>(
        &mut self,
        tx: &Transaction,
        tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        if self.0.accept_tx(tx, tx_inputs).map_err(Either::Left)? {
            self.1.accept_tx(tx, tx_inputs).map_err(Either::Right)
        } else {
            Ok(false)
        }
    }

    type ModifyCoinbaseError =
        Either<C0::ModifyCoinbaseError, C1::ModifyCoinbaseError>;

    fn modify_coinbase(
        &self,
        mut coinbase_txouts: Vec<TxOut>,
    ) -> Result<Vec<TxOut>, Self::ModifyCoinbaseError> {
        coinbase_txouts = self
            .0
            .modify_coinbase(coinbase_txouts)
            .map_err(Either::Left)?;
        self.1
            .modify_coinbase(coinbase_txouts)
            .map_err(Either::Right)
    }

    type HandleBlockHashMessageError = Either<
        C0::HandleBlockHashMessageError,
        C1::HandleBlockHashMessageError,
    >;

    async fn handle_block_hash_msg(
        &mut self,
        msg: BlockHashMessage,
    ) -> Result<(), Self::HandleBlockHashMessageError> {
        let () = self
            .0
            .handle_block_hash_msg(msg)
            .map_err(Either::Left)
            .await?;
        self.1
            .handle_block_hash_msg(msg)
            .map_err(Either::Right)
            .await
    }
}

#[derive(Clone, Copy, Debug)]

pub struct DefaultEnforcer;

impl CusfEnforcer for DefaultEnforcer {
    type AcceptTxError = Infallible;

    fn accept_tx<TxRef>(
        &mut self,
        _tx: &Transaction,
        _tx_inputs: &HashMap<Txid, TxRef>,
    ) -> Result<bool, Self::AcceptTxError>
    where
        TxRef: Borrow<Transaction>,
    {
        Ok(true)
    }

    type ModifyCoinbaseError = Infallible;

    fn modify_coinbase(
        &self,
        coinbase_txouts: Vec<TxOut>,
    ) -> Result<Vec<TxOut>, Self::ModifyCoinbaseError> {
        Ok(coinbase_txouts)
    }

    type HandleBlockHashMessageError = Infallible;

    async fn handle_block_hash_msg(
        &mut self,
        _msg: BlockHashMessage,
    ) -> Result<(), Self::HandleBlockHashMessageError> {
        Ok(())
    }
}
