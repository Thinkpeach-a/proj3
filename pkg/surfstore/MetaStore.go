package surfstore

import (
	context "context"
	//"log"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	mtx            sync.Mutex
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	var curVersion int32
	// if not exist, then set curVersion to zero
	m.mtx.Lock()
	if _, ok := m.FileMetaMap[fileMetaData.Filename]; !ok {
		curVersion = 0
	} else {
		curMetaData := m.FileMetaMap[fileMetaData.Filename]
		curVersion = curMetaData.Version
	}
	updateVersion := fileMetaData.Version
	// do we need to responsible for new files?
	if updateVersion >= curVersion+1 {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	} else {
		return &Version{Version: -1}, nil
	}
	m.mtx.Unlock()
	return &Version{Version: updateVersion}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	//log.Println(m.BlockStoreAddr)
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
