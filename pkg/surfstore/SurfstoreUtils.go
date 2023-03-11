package surfstore

import (
	"fmt"
	"log"
	// "os"
	reflect "reflect"
)

// Implement the logic for a client syncing with the server here.
// type RPCClient struct {
// 	MetaStoreAddr string
// 	BaseDir       string
// 	BlockSize     int
// }

func ClientSync(client RPCClient) {
	baseDir := client.BaseDir
	blockSize := client.BlockSize
	metaStoreAddr := client.MetaStoreAddrs[0]
	
	fmt.Println("baseDir:",baseDir, " blockSize:", blockSize, " metaStoreAddr:", metaStoreAddr)
	
	//Step:1 scan the base directory and form fileMetaMapMetaDir
	fileMetaMapMetaDir, err := LoadMetaFromMetaDirectory(baseDir, blockSize)
	if err != nil {
		log.Println("Error while loading Meta from local directory:", err)
	}

	// Step: 2 form Meta Map from indexes.db
	clientFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Println("Error while loading Meta from local indexs.db file:", err)
	}

	// log.Println("***********Initial fileMetaMapMetaDir***************")
	// PrintMetaMap(fileMetaMapMetaDir)
	// log.Println("*******************************************")

	// log.Println("***********Initial clientFileInfoMap***************")
	// PrintMetaMap(clientFileInfoMap)
	// log.Println("*******************************************")

	// Step: 3(a) compare fileMetaMapMetaDir and fileMetaMapMetaFile
	for filename, metaDataMetaDir := range fileMetaMapMetaDir {
		_, found := clientFileInfoMap[filename]
		if !found {
			clientFileInfoMap[filename] = metaDataMetaDir
		} else {
			hashListMetaDir := metaDataMetaDir.BlockHashList
			hashListMetaFile := clientFileInfoMap[filename].BlockHashList
			if !reflect.DeepEqual(hashListMetaDir, hashListMetaFile) {
				clientFileInfoMap[filename].BlockHashList = hashListMetaDir
				clientFileInfoMap[filename].Version++
			}
		}
	}
	log.Println("**************clientFileInfoMap after reconciling with fileMetaMapMetaFile for updates****************")
	PrintMetaMap(clientFileInfoMap)
	log.Println("*******************************************")

	// Step: 3(B) handling deletes
	for filename, clientMetaData := range clientFileInfoMap {
		_, found := fileMetaMapMetaDir[filename]
		if !found {
			if len(clientMetaData.BlockHashList) == 1 && clientMetaData.BlockHashList[0] == "0"{
				log.Println("already marked as delete")
			} else {
				clientFileInfoMap[filename].BlockHashList = []string{"0"}
				clientFileInfoMap[filename].Version++
			}
			
			
			
		}
	}
	log.Println("**************clientFileInfoMap after reconciling with fileMetaMapMetaFile for deletes***************")
	PrintMetaMap(clientFileInfoMap)
	log.Println("*******************************************")

	// Step: 4 get serverFileInfoMap
	serverFileInfoMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&serverFileInfoMap)
	if err != nil {
		log.Println("Error while getting server file info map", err)
	}
	log.Println("**************Initial serverFileInfoMap***************")
	PrintMetaMap(serverFileInfoMap)
	log.Println("*******************************************")

	// Step: 5 Compare serverFileInfoMap with clientFileInfoMap
	// Step: 5(a) clientFileInfoMap has new/updated files, so upload them in the server and then update serverFileInfoMap
	for filename, metaData := range clientFileInfoMap {
		_, found := serverFileInfoMap[filename]
		log.Println("filename:", filename," found on server?:", found," clientFileInfoMap[filename].Version:",clientFileInfoMap[filename].Version)
		if !found || (clientFileInfoMap[filename].Version >= serverFileInfoMap[filename].Version + 1) {
			log.Println("Uplaoding the file on server")
			err = ServerFileUpload(metaData, baseDir + "/" + filename, client)
			if err != nil {
				log.Println("Error while Uplaoding the file on server")
			} 
		}
	}

	err = client.GetFileInfoMap(&serverFileInfoMap)
	if err != nil {
		log.Println("Error while getting server file info map", err)
	}

	log.Println("**************clientFileInfoMap after reconciling with serverFileInfoMap for update/new/delete file***************")
	PrintMetaMap(clientFileInfoMap)
	log.Println("*******************************************")
	log.Println("**************serverFileInfoMap after reconciling with clientFileInfoMap for update/new/delete file***************")
	PrintMetaMap(serverFileInfoMap)
	log.Println("*******************************************")

	// log.Println("***********serverFileInfoMap has new/updated files, so download them in the clients local and then update clientFileInfoMap")
	// Step: 5(b) serverFileInfoMap has new/updated files, so download them in the clients local and then update clientFileInfoMap
	for filename, serverMetaData := range serverFileInfoMap {
		clientMetaData, found := clientFileInfoMap[filename]
		log.Println("filename:", filename," found on client?:", found," serverFileInfoMap[filename].Version:",serverFileInfoMap[filename].Version)
		if !found || (clientFileInfoMap[filename].Version < serverFileInfoMap[filename].Version) || (!reflect.DeepEqual(clientFileInfoMap[filename].BlockHashList, serverFileInfoMap[filename].BlockHashList)) {
			if !found{
				clientFileInfoMap[filename] = &FileMetaData{}
				clientMetaData = clientFileInfoMap[filename]
			} else {
				log.Println("filename:", filename," clientFileInfoMap[filename].Version:",clientFileInfoMap[filename].Version, "(!reflect.DeepEqual(clientFileInfoMap[filename].BlockHashList, serverFileInfoMap[filename].BlockHashList):", (!reflect.DeepEqual(clientFileInfoMap[filename].BlockHashList, serverFileInfoMap[filename].BlockHashList)))
			}
			err := ClientFileDownload(serverMetaData, clientMetaData, baseDir + "/" + filename, client)
			if err != nil{
				log.Println("Error while downloading file from server to client local:", err)
			} else {
				log.Println("File downloaded to local")
			}
		}
	}

	log.Println("**************clientFileInfoMap after reconciling with serverFileInfoMap for update/new/delete file***************")
	PrintMetaMap(clientFileInfoMap)
	log.Println("*******************************************")

	// Update clients index.db
	WriteMetaFile(clientFileInfoMap, baseDir)
}
