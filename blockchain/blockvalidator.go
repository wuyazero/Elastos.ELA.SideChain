package blockchain

import (
	"errors"
	"math"
	"math/big"
	"time"

	"github.com/wuyazero/Elastos.ELA.SideChain/config"
	. "github.com/wuyazero/Elastos.ELA.SideChain/core"
	. "github.com/wuyazero/Elastos.ELA.SideChain/errors"

	. "github.com/wuyazero/Elastos.ELA.Utility/common"
	"github.com/wuyazero/Elastos.ELA.Utility/crypto"
)

const (
	MaxTimeOffsetSeconds = 2 * 60 * 60
)

func PowCheckBlockSanity(block *Block, powLimit *big.Int, timeSource MedianTimeSource) error {
	header := block.Header

	// A block's main chain block header must contain in spv module
	//mainChainBlockHash := header.SideAuxPow.MainBlockHeader.Hash()
	//if err := spv.VerifyElaHeader(&mainChainBlockHash); err != nil {
	//	return err
	//}

	if !header.SideAuxPow.SideAuxPowCheck(header.Hash()) {
		return errors.New("[PowCheckBlockSanity] block check proof is failed")
	}
	if CheckProofOfWork(&header, powLimit) != nil {
		return errors.New("[PowCheckBlockSanity] block check proof is failed.")
	}

	// A block timestamp must not have a greater precision than one second.
	tempTime := time.Unix(int64(header.Timestamp), 0)
	if !tempTime.Equal(time.Unix(tempTime.Unix(), 0)) {
		return errors.New("[PowCheckBlockSanity] block timestamp of has a higher precision than one second")
	}

	// Ensure the block time is not too far in the future.
	maxTimestamp := timeSource.AdjustedTime().Add(time.Second * MaxTimeOffsetSeconds)
	if tempTime.After(maxTimestamp) {
		return errors.New("[PowCheckBlockSanity] block timestamp of is too far in the future")
	}

	// A block must have at least one transaction.
	numTx := len(block.Transactions)
	if numTx == 0 {
		return errors.New("[PowCheckBlockSanity]  block does not contain any transactions")
	}

	// A block must not have more transactions than the max block payload.
	if numTx > config.Parameters.MaxTxInBlock {
		return errors.New("[PowCheckBlockSanity]  block contains too many transactions")
	}

	// A block must not exceed the maximum allowed block payload when serialized.
	blockSize := block.GetSize()
	if blockSize > config.Parameters.MaxBlockSize {
		return errors.New("[PowCheckBlockSanity] serialized block is too big")
	}

	transactions := block.Transactions
	var rewardInCoinbase = Fixed64(0)
	var totalTxFee = Fixed64(0)
	for index, tx := range transactions {
		// The first transaction in a block must be a coinbase.
		if index == 0 {
			if !tx.IsCoinBaseTx() {
				return errors.New("[PowCheckBlockSanity] first transaction in block is not a coinbase")
			}
			// Calculate reward in coinbase
			for _, output := range tx.Outputs {
				rewardInCoinbase += output.Value
			}
			continue
		}

		// A block must not have more than one coinbase.
		if tx.IsCoinBaseTx() {
			return errors.New("[PowCheckBlockSanity] block contains second coinbase")
		}

		// Calculate transaction fee
		totalTxFee += GetTxFee(tx, DefaultLedger.Blockchain.AssetID)
	}

	// Reward in coinbase must match total transaction fee
	if rewardInCoinbase != totalTxFee {
		return errors.New("[PowCheckBlockSanity] reward amount in coinbase not correct")
	}

	txIds := make([]Uint256, 0, len(transactions))
	existingTxIds := make(map[Uint256]struct{})
	existingTxInputs := make(map[string]struct{})
	existingMainTxs := make(map[Uint256]struct{})
	for _, txn := range transactions {
		txId := txn.Hash()
		// Check for duplicate transactions.
		if _, exists := existingTxIds[txId]; exists {
			return errors.New("[PowCheckBlockSanity] block contains duplicate transaction")
		}
		existingTxIds[txId] = struct{}{}

		// Check for transaction sanity
		if errCode := CheckTransactionSanity(txn); errCode != Success {
			return errors.New("CheckTransactionSanity failed when verifiy block")
		}

		// Check for duplicate UTXO inputs in a block
		for _, input := range txn.Inputs {
			referKey := input.ReferKey()
			if _, exists := existingTxInputs[referKey]; exists {
				return errors.New("[PowCheckBlockSanity] block contains duplicate UTXO")
			}
			existingTxInputs[referKey] = struct{}{}
		}

		if txn.IsRechargeToSideChainTx() {
			rechargePayload := txn.Payload.(*PayloadRechargeToSideChain)
			// Check for duplicate mainchain tx in a block
			hash, err := rechargePayload.GetMainchainTxHash()
			if err != nil {
				return err
			}
			if _, exists := existingMainTxs[*hash]; exists {
				return errors.New("[PowCheckBlockSanity] block contains duplicate mainchain Tx")
			}
			existingMainTxs[*hash] = struct{}{}
		}

		// Append transaction to list
		txIds = append(txIds, txId)
	}
	calcTransactionsRoot, err := crypto.ComputeRoot(txIds)
	if err != nil {
		return errors.New("[PowCheckBlockSanity] merkleTree compute failed")
	}
	if !header.MerkleRoot.IsEqual(calcTransactionsRoot) {
		return errors.New("[PowCheckBlockSanity] block merkle root is invalid")
	}

	return nil
}

func PowCheckBlockContext(block *Block, prevNode *BlockNode, ledger *Ledger) error {
	// The genesis block is valid by definition.
	if prevNode == nil {
		return nil
	}

	header := block.Header
	expectedDifficulty, err := CalcNextRequiredDifficulty(prevNode,
		time.Unix(int64(header.Timestamp), 0))
	if err != nil {
		return err
	}

	if header.Bits != expectedDifficulty {
		return errors.New("block difficulty is not the expected")
	}

	// Ensure the timestamp for the block header is after the
	// median time of the last several blocks (medianTimeBlocks).
	medianTime := CalcPastMedianTime(prevNode)
	tempTime := time.Unix(int64(header.Timestamp), 0)

	if !tempTime.After(medianTime) {
		return errors.New("block timestamp is not after expected")
	}

	// The height of this block is one more than the referenced
	// previous block.
	blockHeight := prevNode.Height + 1

	// Ensure all transactions in the block are finalized.
	for _, txn := range block.Transactions[1:] {
		if !IsFinalizedTransaction(txn, blockHeight) {
			return errors.New("block contains unfinalized transaction")
		}
	}

	return nil
}

func CheckProofOfWork(header *Header, powLimit *big.Int) error {
	// The target difficulty must be larger than zero.
	target := CompactToBig(header.Bits)
	if target.Sign() <= 0 {
		return errors.New("[BlockValidator], block target difficulty is too low.")
	}

	// The target difficulty must be less than the maximum allowed.
	if target.Cmp(powLimit) > 0 {
		return errors.New("[BlockValidator], block target difficulty is higher than max of limit.")
	}

	// The block hash must be less than the claimed target.
	hash := header.SideAuxPow.MainBlockHeader.AuxPow.ParBlockHeader.Hash()

	hashNum := HashToBig(&hash)
	if hashNum.Cmp(target) > 0 {
		return errors.New("[BlockValidator], block target difficulty is higher than expected difficulty.")
	}

	return nil
}

func IsFinalizedTransaction(msgTx *Transaction, blockHeight uint32) bool {
	// Lock time of zero means the transaction is finalized.
	lockTime := msgTx.LockTime
	if lockTime == 0 {
		return true
	}

	//FIXME only height
	if lockTime < blockHeight {
		return true
	}

	// At this point, the transaction's lock time hasn't occurred yet, but
	// the transaction might still be finalized if the sequence number
	// for all transaction inputs is maxed out.
	for _, txIn := range msgTx.Inputs {
		if txIn.Sequence != math.MaxUint16 {
			return false
		}
	}
	return true
}
