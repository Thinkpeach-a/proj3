package surfstore

import (
	context "context"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	block := bs.BlockMap[blockHash.Hash]
	return block, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.BlockData)
	bs.BlockMap[hash] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hash_out []string
	for _, cur_hash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[cur_hash]; ok {
			hash_out = append(hash_out, cur_hash)
		}
	}
	return &BlockHashes{Hashes: hash_out}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
