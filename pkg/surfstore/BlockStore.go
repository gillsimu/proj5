package surfstore

import (
	context "context"
	"fmt"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

//The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier.
//This service stores these blocks, and when given an identifier, retrieves and returns the appropriate block.

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
	mutex sync.Mutex
}

func (bs *BlockStore) InvokeLock(ctx context.Context) {
	bs.mutex.Lock();
}

func (bs *BlockStore) InvokeUnLock(ctx context.Context) {
	bs.mutex.Unlock();
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.InvokeLock(ctx);
	uid := blockHash.GetHash();
	if val, found := bs.BlockMap[uid]; found {
		bs.InvokeUnLock(ctx);
		return val, nil
	}
	bs.InvokeUnLock(ctx);
	return nil, fmt.Errorf("Block %v is not found in the map", blockHash.GetHash())
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.InvokeLock(ctx);
	bs.BlockMap[GetBlockHashString(block.BlockData)] =  block
	bs.InvokeUnLock(ctx);
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.InvokeLock(ctx);
	subsetHashes := []string{}
	for _, hash := range blockHashesIn.Hashes {
		_, found := bs.BlockMap[hash]
		if found {
			subsetHashes = append(subsetHashes, hash)
		}
	}
	bs.InvokeUnLock(ctx);
	return &BlockHashes{Hashes: subsetHashes}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	subsetBlockHashes := []string{}
	for hash := range bs.BlockMap {
		subsetBlockHashes = append(subsetBlockHashes, hash)
	}
	return &BlockHashes{Hashes: subsetBlockHashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
