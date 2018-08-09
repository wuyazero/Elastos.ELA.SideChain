package core

import (
	"github.com/elastos/Elastos.ELA.SideChain/core/types"

	. "github.com/elastos/Elastos.ELA.Utility/common"
)

// IChainStore provides func with store package.
type IChainStore interface {
	InitWithGenesisBlock(genesisblock *types.Block) (uint32, error)

	SaveBlock(b *types.Block) error
	GetBlock(hash Uint256) (*types.Block, error)
	GetBlockHash(height uint32) (Uint256, error)
	IsDoubleSpend(tx *types.Transaction) bool

	GetHeader(hash Uint256) (*types.Header, error)

	RollbackBlock(hash Uint256) error

	GetTransaction(txId Uint256) (*types.Transaction, uint32, error)
	GetTxReference(tx *types.Transaction) (map[*types.Input]*types.Output, error)

	PersistAsset(assetid Uint256, asset types.Asset) error
	GetAsset(hash Uint256) (*types.Asset, error)

	PersistMainchainTx(mainchainTxHash Uint256)
	GetMainchainTx(mainchainTxHash Uint256) (byte, error)

	GetCurrentBlockHash() Uint256
	GetHeight() uint32

	RemoveHeaderListElement(hash Uint256)

	GetUnspent(txid Uint256, index uint16) (*types.Output, error)
	ContainsUnspent(txid Uint256, index uint16) (bool, error)
	GetUnspentFromProgramHash(programHash Uint168, assetid Uint256) ([]*types.UTXO, error)
	GetUnspentsFromProgramHash(programHash Uint168) (map[Uint256][]*types.UTXO, error)
	GetAssets() map[Uint256]*types.Asset

	IsTxHashDuplicate(txhash Uint256) bool
	IsMainchainTxHashDuplicate(mainchainTxHash Uint256) bool
	IsBlockInStore(hash Uint256) bool
	Close()
}
