package store

import (
	"bytes"
	"container/list"
	"errors"
	"sync"
	"time"

	"github.com/elastos/Elastos.ELA.SideChain/core"
	"github.com/elastos/Elastos.ELA.SideChain/core/types"
	"github.com/elastos/Elastos.ELA.SideChain/events"
	"github.com/elastos/Elastos.ELA.SideChain/log"

	. "github.com/elastos/Elastos.ELA.Utility/common"
	)

const ValueNone = 0
const ValueExist = 1

const TaskChanCap = 4

var (
	ErrDBNotFound = errors.New("leveldb: not found")
)

type persistTask interface{}

type rollbackBlockTask struct {
	blockHash Uint256
	reply     chan bool
}

type persistBlockTask struct {
	block *types.Block
	reply chan bool
}

type ChainStore struct {
	db IStore

	taskCh chan persistTask
	quit   chan chan bool

	mu          sync.RWMutex // guard the following var
	headerIndex map[uint32]Uint256
	headerCache map[Uint256]*types.Header
	headerIdx   *list.List

	currentBlockHeight uint32
	storedHeaderCount  uint32
}

func NewChainStore(db IStore) core.IChainStore {
	store := &ChainStore{
		db:                 db,
		headerIndex:        map[uint32]Uint256{},
		headerCache:        map[Uint256]*types.Header{},
		headerIdx:          list.New(),
		currentBlockHeight: 0,
		storedHeaderCount:  0,
		taskCh:             make(chan persistTask, TaskChanCap),
		quit:               make(chan chan bool, 1),
	}

	go store.loop()

	return store
}

func (c *ChainStore) Close() {
	closed := make(chan bool)
	c.quit <- closed
	<-closed

	c.db.Close()
}

func (c *ChainStore) loop() {
	for {
		select {
		case t := <-c.taskCh:
			now := time.Now()
			switch task := t.(type) {
			case *persistBlockTask:
				c.handlePersistBlockTask(task.block)
				task.reply <- true
				tcall := float64(time.Now().Sub(now)) / float64(time.Second)
				log.Debugf("handle block exetime: %g num transactions:%d", tcall, len(task.block.Transactions))
			case *rollbackBlockTask:
				c.handleRollbackBlockTask(task.blockHash)
				task.reply <- true
				tcall := float64(time.Now().Sub(now)) / float64(time.Second)
				log.Debugf("handle block rollback exetime: %g", tcall)
			}

		case closed := <-c.quit:
			closed <- true
			return
		}
	}
}

// can only be invoked by backend write goroutine
func (c *ChainStore) clearCache(b *types.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for e := c.headerIdx.Front(); e != nil; e = e.Next() {
		n := e.Value.(types.Header)
		h := n.Hash()
		if h.IsEqual(b.Hash()) {
			c.headerIdx.Remove(e)
		}
	}
}

func (c *ChainStore) InitWithGenesisBlock(genesisBlock *types.Block) (uint32, error) {
	prefix := []byte{byte(CFG_Version)}
	version, err := c.db.Get(prefix)
	if err != nil {
		version = []byte{0x00}
	}

	if version[0] == 0x00 {
		// batch delete old data
		c.db.NewBatch()
		iter := c.db.NewIterator(nil)
		for iter.Next() {
			c.db.BatchDelete(iter.Key())
		}
		iter.Release()

		err := c.db.BatchCommit()
		if err != nil {
			return 0, err
		}

		// persist genesis block
		err = c.persist(genesisBlock)
		if err != nil {
			return 0, err
		}
		// put version to db
		err = c.db.Put(prefix, []byte{0x01})
		if err != nil {
			return 0, err
		}
	}

	// GenesisBlock should exist in chain
	// Or the bookkeepers are not consistent with the chain
	hash := genesisBlock.Hash()
	if !c.IsBlockInStore(hash) {
		return 0, errors.New("genesis block is not consistent with the chain")
	}
	core.DefaultChain.GenesisHash = hash

	// Get Current Block
	currentBlockPrefix := []byte{byte(SYS_CurrentBlock)}
	data, err := c.db.Get(currentBlockPrefix)
	if err != nil {
		return 0, err
	}

	r := bytes.NewReader(data)
	var blockHash Uint256
	blockHash.Deserialize(r)
	c.currentBlockHeight, err = ReadUint32(r)
	endHeight := c.currentBlockHeight

	startHeight := uint32(0)
	if endHeight > core.MinMemoryNodes {
		startHeight = endHeight - core.MinMemoryNodes
	}

	for start := startHeight; start <= endHeight; start++ {
		hash, err := c.GetBlockHash(start)
		if err != nil {
			return 0, err
		}
		header, err := c.GetHeader(hash)
		if err != nil {
			return 0, err
		}
		node, err := core.DefaultChain.LoadBlockNode(header, &hash)
		if err != nil {
			return 0, err
		}

		// This node is now the end of the best chain.
		core.DefaultChain.BestChain = node
	}

	return c.currentBlockHeight, nil

}

func (c *ChainStore) IsTxHashDuplicate(txhash Uint256) bool {
	prefix := []byte{byte(DATA_Transaction)}
	_, err_get := c.db.Get(append(prefix, txhash.Bytes()...))
	if err_get != nil {
		return false
	} else {
		return true
	}
}

func (c *ChainStore) IsDoubleSpend(txn *types.Transaction) bool {
	if len(txn.Inputs) == 0 {
		return false
	}

	unspentPrefix := []byte{byte(IX_Unspent)}
	for i := 0; i < len(txn.Inputs); i++ {
		txhash := txn.Inputs[i].Previous.TxID
		unspentValue, err_get := c.db.Get(append(unspentPrefix, txhash.Bytes()...))
		if err_get != nil {
			return true
		}

		unspents, _ := GetUint16Array(unspentValue)
		findFlag := false
		for k := 0; k < len(unspents); k++ {
			if unspents[k] == txn.Inputs[i].Previous.Index {
				findFlag = true
				break
			}
		}

		if !findFlag {
			return true
		}
	}

	return false
}

func (c *ChainStore) IsMainchainTxHashDuplicate(mainchainTxHash Uint256) bool {
	prefix := []byte{byte(IX_MainChain_Tx)}
	_, err := c.db.Get(append(prefix, mainchainTxHash.Bytes()...))
	if err != nil {
		return false
	} else {
		return true
	}
}

func (c *ChainStore) GetBlockHash(height uint32) (Uint256, error) {
	queryKey := bytes.NewBuffer(nil)
	queryKey.WriteByte(byte(DATA_BlockHash))
	err := WriteUint32(queryKey, height)

	if err != nil {
		return Uint256{}, err
	}
	blockHash, err := c.db.Get(queryKey.Bytes())
	if err != nil {
		//TODO: implement error process
		return Uint256{}, err
	}
	blockHash256, err := Uint256FromBytes(blockHash)
	if err != nil {
		return Uint256{}, err
	}

	return *blockHash256, nil
}

func (c *ChainStore) getHeaderWithCache(hash Uint256) *types.Header {
	for e := c.headerIdx.Front(); e != nil; e = e.Next() {
		n := e.Value.(types.Header)
		eh := n.Hash()
		if eh.IsEqual(hash) {
			return &n
		}
	}

	h, _ := c.GetHeader(hash)

	return h
}

func (c *ChainStore) GetCurrentBlockHash() Uint256 {
	hash, err := c.GetBlockHash(c.currentBlockHeight)
	if err != nil {
		return Uint256{}
	}

	return hash
}

func (c *ChainStore) RollbackBlock(blockHash Uint256) error {

	reply := make(chan bool)
	c.taskCh <- &rollbackBlockTask{blockHash: blockHash, reply: reply}
	<-reply

	return nil
}

func (c *ChainStore) GetHeader(hash Uint256) (*types.Header, error) {
	var h = new(types.Header)

	prefix := []byte{byte(DATA_Header)}
	data, err := c.db.Get(append(prefix, hash.Bytes()...))
	if err != nil {
		//TODO: implement error process
		return nil, err
	}

	r := bytes.NewReader(data)
	// first 8 bytes is sys_fee
	_, err = ReadUint64(r)
	if err != nil {
		return nil, err
	}

	// Deserialize block data
	err = h.Deserialize(r)
	if err != nil {
		return nil, err
	}

	return h, err
}

func (c *ChainStore) PersistAsset(assetId Uint256, asset types.Asset) error {
	w := bytes.NewBuffer(nil)

	asset.Serialize(w)

	// generate key
	assetKey := bytes.NewBuffer(nil)
	// add asset prefix.
	assetKey.WriteByte(byte(ST_Info))
	// contact asset id
	assetId.Serialize(assetKey)

	log.Debugf("asset key: %x", assetKey)

	// PUT VALUE
	c.db.BatchPut(assetKey.Bytes(), w.Bytes())

	return nil
}

func (c *ChainStore) GetAsset(hash Uint256) (*types.Asset, error) {
	log.Debugf("GetAsset Hash: %s", hash.String())

	asset := new(types.Asset)
	prefix := []byte{byte(ST_Info)}
	data, err := c.db.Get(append(prefix, hash.Bytes()...))

	log.Debugf("GetAsset Data: %s", data)
	if err != nil {
		return nil, err
	}

	asset.Deserialize(bytes.NewReader(data))

	return asset, nil
}

func (c *ChainStore) PersistMainchainTx(mainchainTxHash Uint256) {
	key := []byte{byte(IX_MainChain_Tx)}
	key = append(key, mainchainTxHash.Bytes()...)

	// PUT VALUE
	c.db.BatchPut(key, []byte{byte(ValueExist)})
}

func (c *ChainStore) GetMainchainTx(mainchainTxHash Uint256) (byte, error) {
	key := []byte{byte(IX_MainChain_Tx)}
	data, err := c.db.Get(append(key, mainchainTxHash.Bytes()...))
	if err != nil {
		return ValueNone, err
	}

	return data[0], nil
}

func (c *ChainStore) GetTransaction(txId Uint256) (*types.Transaction, uint32, error) {
	key := append([]byte{byte(DATA_Transaction)}, txId.Bytes()...)
	value, err := c.db.Get(key)
	if err != nil {
		return nil, 0, err
	}

	r := bytes.NewReader(value)
	height, err := ReadUint32(r)
	if err != nil {
		return nil, 0, err
	}

	var txn types.Transaction
	if err := txn.Deserialize(r); err != nil {
		return nil, height, err
	}

	return &txn, height, nil
}

func (c *ChainStore) GetTxReference(tx *types.Transaction) (map[*types.Input]*types.Output, error) {
	if tx.TxType == types.RegisterAsset {
		return nil, nil
	}
	//UTXO input /  Outputs
	reference := make(map[*types.Input]*types.Output)
	// Key index，v UTXOInput
	for _, utxo := range tx.Inputs {
		transaction, _, err := c.GetTransaction(utxo.Previous.TxID)
		if err != nil {
			return nil, errors.New("GetTxReference failed, previous transaction not found")
		}
		index := utxo.Previous.Index
		if int(index) >= len(transaction.Outputs) {
			return nil, errors.New("GetTxReference failed, refIdx out of range.")
		}
		reference[utxo] = transaction.Outputs[index]
	}
	return reference, nil
}

func (c *ChainStore) PersistTransaction(tx *types.Transaction, height uint32) error {
	// generate key with DATA_Transaction prefix
	key := new(bytes.Buffer)
	// add transaction header prefix.
	key.WriteByte(byte(DATA_Transaction))
	// get transaction hash
	hash := tx.Hash()
	if err := hash.Serialize(key); err != nil {
		return err
	}
	log.Debugf("transaction header + hash: %x", key)

	// generate value
	value := new(bytes.Buffer)
	WriteUint32(value, height)
	if err := tx.Serialize(value); err != nil {
		return err
	}
	log.Debugf("transaction tx data: %x", value)

	// put value
	c.db.BatchPut(key.Bytes(), value.Bytes())

	return nil
}

func (c *ChainStore) GetBlock(hash Uint256) (*types.Block, error) {
	var b = new(types.Block)
	prefix := []byte{byte(DATA_Header)}
	bHash, err := c.db.Get(append(prefix, hash.Bytes()...))
	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(bHash)

	// first 8 bytes is sys_fee
	_, err = ReadUint64(r)
	if err != nil {
		return nil, err
	}

	// Deserialize block data
	if err := b.FromTrimmedData(r); err != nil {
		return nil, err
	}

	// Deserialize transaction
	for i, txn := range b.Transactions {
		tmp, _, err := c.GetTransaction(txn.Hash())
		if err != nil {
			return nil, err
		}
		b.Transactions[i] = tmp
	}

	return b, nil
}

func (c *ChainStore) rollback(b *types.Block) error {
	c.db.NewBatch()
	c.RollbackTrimmedBlock(b)
	c.RollbackBlockHash(b)
	c.RollbackTransactions(b)
	c.RollbackUnspendUTXOs(b)
	c.RollbackUnspend(b)
	c.RollbackCurrentBlock(b)
	c.db.BatchCommit()

	core.DefaultChain.UpdateBestHeight(b.Header.Height - 1)
	c.mu.Lock()
	c.currentBlockHeight = b.Header.Height - 1
	c.mu.Unlock()

	core.DefaultChain.BCEvents.Notify(events.EventRollbackTransaction, b)

	return nil
}

func (c *ChainStore) persist(b *types.Block) error {
	c.db.NewBatch()
	if err := c.PersistTrimmedBlock(b); err != nil {
		return err
	}
	if err := c.PersistBlockHash(b); err != nil {
		return err
	}
	if err := c.PersistTransactions(b); err != nil {
		return err
	}
	if err := c.PersistUnspendUTXOs(b); err != nil {
		return err
	}
	if err := c.PersistUnspend(b); err != nil {
		return err
	}
	if err := c.PersistCurrentBlock(b); err != nil {
		return err
	}
	return c.db.BatchCommit()
}

// can only be invoked by backend write goroutine
func (c *ChainStore) addHeader(header *types.Header) {

	log.Debugf("addHeader(), Height=%d", header.Height)

	hash := header.Hash()

	c.mu.Lock()
	c.headerCache[header.Hash()] = header
	c.headerIndex[header.Height] = hash
	c.headerIdx.PushBack(*header)
	c.mu.Unlock()

	log.Debug("[addHeader]: finish, header height:", header.Height)
}

func (c *ChainStore) SaveBlock(b *types.Block) error {
	log.Debug("SaveBlock()")

	reply := make(chan bool)
	c.taskCh <- &persistBlockTask{block: b, reply: reply}
	<-reply

	return nil
}

func (c *ChainStore) handleRollbackBlockTask(blockHash Uint256) {
	block, err := c.GetBlock(blockHash)
	if err != nil {
		log.Errorf("block %x can't be found", BytesToHexString(blockHash.Bytes()))
		return
	}
	c.rollback(block)
}

func (c *ChainStore) handlePersistBlockTask(b *types.Block) {
	if b.Header.Height <= c.currentBlockHeight {
		return
	}

	c.persistBlock(b)
	c.clearCache(b)
}

func (c *ChainStore) persistBlock(block *types.Block) {
	err := c.persist(block)
	if err != nil {
		log.Fatal("[persistBlocks]: error to persist block:", err.Error())
		return
	}

	core.DefaultChain.UpdateBestHeight(block.Header.Height)
	c.mu.Lock()
	c.currentBlockHeight = block.Header.Height
	c.mu.Unlock()

	core.DefaultChain.BCEvents.Notify(events.EventBlockPersistCompleted, block)
}

func (c *ChainStore) GetUnspent(txid Uint256, index uint16) (*types.Output, error) {
	if ok, _ := c.ContainsUnspent(txid, index); ok {
		tx, _, err := c.GetTransaction(txid)
		if err != nil {
			return nil, err
		}

		return tx.Outputs[index], nil
	}

	return nil, errors.New("[GetUnspent] NOT ContainsUnspent.")
}

func (c *ChainStore) ContainsUnspent(txid Uint256, index uint16) (bool, error) {
	unspentPrefix := []byte{byte(IX_Unspent)}
	unspentValue, err := c.db.Get(append(unspentPrefix, txid.Bytes()...))

	if err != nil {
		return false, err
	}

	unspentArray, err := GetUint16Array(unspentValue)
	if err != nil {
		return false, err
	}

	for i := 0; i < len(unspentArray); i++ {
		if unspentArray[i] == index {
			return true, nil
		}
	}

	return false, nil
}

func (c *ChainStore) RemoveHeaderListElement(hash Uint256) {
	for e := c.headerIdx.Front(); e != nil; e = e.Next() {
		n := e.Value.(types.Header)
		h := n.Hash()
		if h.IsEqual(hash) {
			c.headerIdx.Remove(e)
		}
	}
}

func (c *ChainStore) GetHeight() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.currentBlockHeight
}

func (c *ChainStore) IsBlockInStore(hash Uint256) bool {
	var b = new(types.Block)
	prefix := []byte{byte(DATA_Header)}
	log.Debug("Get block key: ", BytesToHexString(append(prefix, hash.Bytes()...)))
	blockData, err := c.db.Get(append(prefix, hash.Bytes()...))
	if err != nil {
		return false
	}

	r := bytes.NewReader(blockData)

	// first 8 bytes is sys_fee
	_, err = ReadUint64(r)
	if err != nil {
		log.Error("Read sys_fee failed: ", err)
		return false
	}

	// Deserialize block data
	err = b.FromTrimmedData(r)
	if err != nil {
		log.Error("Get trimmed data failed: ", err)
		return false
	}

	if b.Header.Height > c.currentBlockHeight {
		log.Error("Header height", b.Header.Height, "greater then current height:", c.currentBlockHeight)
		return false
	}

	return true
}

func (c *ChainStore) GetUnspentElementFromProgramHash(programHash Uint168, assetid Uint256, height uint32) ([]*types.UTXO, error) {
	prefix := []byte{byte(IX_Unspent_UTXO)}
	prefix = append(prefix, programHash.Bytes()...)
	prefix = append(prefix, assetid.Bytes()...)

	key := bytes.NewBuffer(prefix)
	if err := WriteUint32(key, height); err != nil {
		return nil, err
	}
	unspentsData, err := c.db.Get(key.Bytes())
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(unspentsData)
	listNum, err := ReadVarUint(r, 0)
	if err != nil {
		return nil, err
	}

	// read unspent list in store
	unspents := make([]*types.UTXO, listNum)
	for i := 0; i < int(listNum); i++ {
		uu := new(types.UTXO)
		err := uu.Deserialize(r)
		if err != nil {
			return nil, err
		}

		unspents[i] = uu
	}

	return unspents, nil
}

func (c *ChainStore) GetUnspentFromProgramHash(programHash Uint168, assetid Uint256) ([]*types.UTXO, error) {
	unspents := make([]*types.UTXO, 0)

	key := []byte{byte(IX_Unspent_UTXO)}
	key = append(key, programHash.Bytes()...)
	key = append(key, assetid.Bytes()...)
	iter := c.db.NewIterator(key)
	for iter.Next() {
		r := bytes.NewReader(iter.Value())
		listNum, err := ReadVarUint(r, 0)
		if err != nil {
			return nil, err
		}

		for i := 0; i < int(listNum); i++ {
			uu := new(types.UTXO)
			err := uu.Deserialize(r)
			if err != nil {
				return nil, err
			}

			unspents = append(unspents, uu)
		}
	}

	return unspents, nil

}

func (c *ChainStore) GetUnspentsFromProgramHash(programHash Uint168) (map[Uint256][]*types.UTXO, error) {
	uxtoUnspents := make(map[Uint256][]*types.UTXO)

	prefix := []byte{byte(IX_Unspent_UTXO)}
	key := append(prefix, programHash.Bytes()...)
	iter := c.db.NewIterator(key)
	for iter.Next() {
		rk := bytes.NewReader(iter.Key())

		// read prefix
		_, _ = ReadBytes(rk, 1)
		var ph Uint168
		ph.Deserialize(rk)
		var assetid Uint256
		assetid.Deserialize(rk)

		r := bytes.NewReader(iter.Value())
		listNum, err := ReadVarUint(r, 0)
		if err != nil {
			return nil, err
		}

		// read unspent list in store
		unspents := make([]*types.UTXO, listNum)
		for i := 0; i < int(listNum); i++ {
			uu := new(types.UTXO)
			err := uu.Deserialize(r)
			if err != nil {
				return nil, err
			}

			unspents[i] = uu
		}
		uxtoUnspents[assetid] = append(uxtoUnspents[assetid], unspents[:]...)
	}

	return uxtoUnspents, nil
}

func (c *ChainStore) PersistUnspentWithProgramHash(programHash Uint168, assetid Uint256, height uint32, unspents []*types.UTXO) error {
	prefix := []byte{byte(IX_Unspent_UTXO)}
	prefix = append(prefix, programHash.Bytes()...)
	prefix = append(prefix, assetid.Bytes()...)
	key := bytes.NewBuffer(prefix)
	if err := WriteUint32(key, height); err != nil {
		return err
	}

	if len(unspents) == 0 {
		c.db.BatchDelete(key.Bytes())
		return nil
	}

	listnum := len(unspents)
	w := bytes.NewBuffer(nil)
	WriteVarUint(w, uint64(listnum))
	for i := 0; i < listnum; i++ {
		unspents[i].Serialize(w)
	}

	// BATCH PUT VALUE
	c.db.BatchPut(key.Bytes(), w.Bytes())

	return nil
}

func (c *ChainStore) GetAssets() map[Uint256]*types.Asset {
	assets := make(map[Uint256]*types.Asset)

	iter := c.db.NewIterator([]byte{byte(ST_Info)})
	for iter.Next() {
		rk := bytes.NewReader(iter.Key())

		// read prefix
		_, _ = ReadBytes(rk, 1)
		var assetid Uint256
		assetid.Deserialize(rk)
		log.Tracef("[GetAssets] assetid: %x", assetid.Bytes())

		asset := new(types.Asset)
		r := bytes.NewReader(iter.Value())
		asset.Deserialize(r)

		assets[assetid] = asset
	}

	return assets
}
