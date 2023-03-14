package surfstore

import (
	context "context"
	"fmt"
	"log"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = b.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tempBlockHash := BlockHashes{
		Hashes: blockHashesIn,
	}
	b, err := c.HasBlocks(ctx, &tempBlockHash)

	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = b.Hashes

	// close the connection
	return conn.Close()
}

func KnownError(err error) bool {
	if err.Error() == ERR_SERVER_CRASHED.Error() || err.Error() == ERR_NOT_LEADER.Error() {
		return true
	}
	return false
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	fmt.Println("-----Getting file InfoMap-----")
	for idx, metaStoreAddr := range surfClient.MetaStoreAddrs {
		fmt.Println("id:", idx, " metaStoreAddr:", metaStoreAddr)
		// connect to the server
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			continue
		}
		*serverFileInfoMap = b.FileInfoMap

		// close the connection
		return conn.Close()
	}
	log.Fatalf("Error while getting fileInfoMap:")
	return UNKOWN_ERROR
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	fmt.Println("-----Update file-----")
	for idx, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		fmt.Println("id:", idx, " metaStoreAddr:", metaStoreAddr)
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		client := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()
		b, err := client.UpdateFile(ctx, fileMetaData)
		if ctx.Err() != nil {
			fmt.Println("Update File Context timed out")
		}
		if err != nil {
			fmt.Println("Error while updating file", err)
			// if(KnownError(err)){
			// 	continue
			// }
			// fmt.Println("Error while getting UpdateFile:", err)
			// conn.Close()
			// return err
			continue
		}
		*latestVersion = b.Version

		// close the connection
		return conn.Close()
	}
	log.Fatalf("Error while getting UpdateFile:")
	return UNKOWN_ERROR
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}

	*blockHashes = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	fmt.Println("-----GetBlockStoreMap-----")
	for idx, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		fmt.Println("id:", idx, " metaStoreAddr:", metaStoreAddr)
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		tempBlockHash := BlockHashes{
			Hashes: blockHashesIn,
		}
		b, err := c.GetBlockStoreMap(ctx, &tempBlockHash)
		if err != nil {
			// if(KnownError(err)){
			// 	continue
			// }
			// fmt.Print("Error while getting GetBlockStoreMap:", err)
			// conn.Close()
			// return err
			continue
		}

		m := make(map[string][]string)

		for key, val := range b.BlockStoreMap {
			hashes := []string{}
			hashes = append(hashes, val.Hashes...)
			m[key] = hashes
		}
		*blockStoreMap = m

		// close the connection
		return conn.Close()
	}
	log.Fatalf("Error while getting GetBlockStoreMap:")
	return UNKOWN_ERROR
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	fmt.Println("-----GetBlockStoreAddrs-----")
	for idx, metaStoreAddr := range surfClient.MetaStoreAddrs {
		// connect to the server
		fmt.Println("id:", idx, " metaStoreAddr:", metaStoreAddr)
		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)
		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		b, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		fmt.Println("Error:", err)
		if err != nil {
			continue
		}
		*blockStoreAddrs = b.BlockStoreAddrs

		// close the connection
		return conn.Close()
	}
	log.Fatalf("Error while getting GetBlockStoreAddrs:")
	return UNKOWN_ERROR
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
