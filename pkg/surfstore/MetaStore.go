package surfstore

import (
	context "context"
	"log"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// The MetaStore service manages the metadata of files and the entire system.
// Most importantly, the MetaStore service holds the mapping of filenames to blocks.
// Furthermore, it should be aware of available BlockStores and map blocks to particular BlockStores.
// In a real deployment, a cloud file service like Dropbox or Google Drive will hold exabytes of data, and so will require 10s of thousands of BlockStores or more to hold all that data.


type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
	mutex sync.Mutex
}

func (m *MetaStore) InvokeLock(ctx context.Context) {
	m.mutex.Lock();
}

func (m *MetaStore) InvokeUnLock(ctx context.Context) {
	m.mutex.Unlock();
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	metaData := m.FileMetaMap
	return &FileInfoMap{FileInfoMap: metaData}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.InvokeLock(ctx);
	filename := fileMetaData.Filename
	newVersion := fileMetaData.Version
	var currentVersion int32

	if metaData, found := m.FileMetaMap[filename]; found {
		currentVersion = metaData.Version
	} else {
		m.FileMetaMap[filename] = fileMetaData
		m.InvokeUnLock(ctx);
		return &Version{Version: newVersion}, nil
	}

	if currentVersion + 1 == newVersion {
		m.FileMetaMap[filename] = fileMetaData
	} else {
		m.InvokeUnLock(ctx);
		return &Version{Version: -1}, nil
	}
	m.InvokeUnLock(ctx);
	return &Version{Version: newVersion}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes )

	for _, hash := range blockHashesIn.Hashes{
		server := m.ConsistentHashRing.GetResponsibleServer(hash)
		log.Println("hash:",hash," server:",server)
		_, found := blockStoreMap[server]
		if !found {
			blockStoreMap[server] = &BlockHashes{}
		}
		blockStoreMap[server].Hashes = append(blockStoreMap[server].Hashes, hash)
	}

	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
