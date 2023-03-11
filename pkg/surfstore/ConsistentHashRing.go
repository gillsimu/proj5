package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
	Hashes []string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	for _, h := range c.Hashes{
		if h > blockId {
			return c.ServerMap[h]
		}
	}
	return c.ServerMap[c.Hashes[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {	// server name: "blockstore" + serverAddr
	// hash servers on hash ring
	var c ConsistentHashRing
	serverMap := make(map[string]string)	// hash: serverName
	hashes := []string{}
	for _, serverName :=  range serverAddrs {
		serverHash := c.Hash("blockstore" + serverName);
		serverMap[serverHash] = serverName
		hashes = append(hashes, serverHash)
	}
	sort.Strings(hashes)
	c.Hashes = hashes
	c.ServerMap = serverMap
	return &c
}
