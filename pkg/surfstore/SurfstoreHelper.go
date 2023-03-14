package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	// "io"
	// "io/fs"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func UploadNewFile( client RPCClient, newVersion int32, clientMetaData *FileMetaData) error {
	log.Println("File not found on server so uplaoding it now")
	err := client.UpdateFile(clientMetaData, &newVersion)
	if err != nil {
		log.Println("Error while uplaoding file to the server", err)
		return err
	}
	
	clientMetaData.Version = newVersion
	return nil
}

func getNumberOfBlocks( filePath string, blockSize int) int {
	fileInfo, _ := os.Stat(filePath)
	return int(math.Ceil((float64(1.0*float64(fileInfo.Size()))/float64(blockSize))))
}

func ServerFileUpload(clientMetaData *FileMetaData, filePath string, client RPCClient) error {
	var blockStoreAddrs []string
	var newVersion int32
	blockSize := client.BlockSize
	err := client.GetBlockStoreAddrs(&blockStoreAddrs)
	if err != nil {
		log.Println("Error while getting block store address", err)
		return err
	}
	
	_, err =  os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		err := UploadNewFile(client, newVersion, clientMetaData)
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		log.Println("Error opening file: ", err)
	}
	defer file.Close()

	blocks := getNumberOfBlocks(filePath, blockSize)
	hashes := []string{}

	hashAndBlock := make(map[string] Block)

	for i:=0; i<blocks; i++ {
		data := make([]byte, blockSize)
		bytesRead, _ := file.Read(data)
		finaldata := data[:bytesRead]
		block := Block{
			BlockSize: int32(bytesRead),
			BlockData: finaldata, 
		}
		hashes = append(hashes, GetBlockHashString(block.BlockData))
		hashAndBlock[GetBlockHashString(block.BlockData)] = block
		
	}

	var blockStoreMap map[string][]string
	log.Println("Fetching the servers for each block hash")
	if err := client.GetBlockStoreMap(hashes, &blockStoreMap); err != nil {
		log.Println("Error while getting block store map", err)
		return err
	}
	log.Println("Fetched the servers for each block hash->", blockStoreMap)
	

	for server, hashes := range blockStoreMap{
		for _, hash := range hashes {
			metaDataBlock := hashAndBlock[hash]
			var succ bool
			err := client.PutBlock(&metaDataBlock, server, &succ)
			if err != nil {
				log.Println("Failed to put block: ", err)
			}
		}
	}

	error := client.UpdateFile(clientMetaData, &newVersion)

	if error != nil {
		log.Println("Failed to update file: ", error)
		clientMetaData.Version = -1
	}
	
	clientMetaData.Version = newVersion
	return nil
}


/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `insert into indexes (fileName, version, hashIndex, hashValue) VALUES (?,?,?,?)`

// WriteMetaFile writes the file meta map back to local metadata file indexes.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {

	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}

	// open conneciton to db
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	statement, _ = db.Prepare(insertTuple)

	for _, metaData := range fileMetas {
		filename := metaData.Filename
		version := metaData.Version
		hashList := metaData.BlockHashList

		for hashIndex, hashValue := range hashList {
			statement.Exec(filename, version, hashIndex+1, hashValue)
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `select distinct(fileName) from indexes`

const getTuplesByFileName string = `select * from indexes where fileName=? ORDER BY hashIndex ASC`

func IndexesDbHandler(indexesDbPath string) error {

	_, err := os.Stat(indexesDbPath)

	if os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
		log.Println("Index.db file didnt exist, therefore creating it now!")
		db, err := sql.Open("sqlite3", indexesDbPath)
		if err != nil {
			log.Fatal("Error while trying to open indexes.db:", err)
		}
		statement, err := db.Prepare(createTable)
		if err != nil {
			log.Fatal("Error while creating indexes.db:", err)
			return err
		}
		statement.Exec()
		db.Close()
		log.Println("Index.db file created successfully!")
	}
	
	return nil 
}

func CreateEntriesInDB(indexesDbPath string) error {
	db, err := sql.Open("sqlite3", indexesDbPath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	defer db.Close()

	statemnet, _ := db.Prepare(insertTuple)
	statemnet.Exec("file1", 1, 1, "1234")
	statemnet.Exec("file1", 1, 2, "1235")
	statemnet.Exec("file1", 1, 3, "1236")
	statemnet.Exec("file1", 1, 4, "1237")
	statemnet.Exec("file2", 1, 1, "4567")
	statemnet.Exec("file3", 1, 2, "9998")
	statemnet.Exec("file3", 1, 1, "9999")
	statemnet.Exec("file4", 2, 1, "1111")
	statemnet.Exec("file5", 3, 1, "2222")
	statemnet.Exec("hello.txt", 1, 1, "20bacb77dc35a8299211f1b09fd32e00848f983668c8fb06a0abc37342149e72")
	statemnet.Exec("hello.txt", 1, 2, "f18a2062a49bdb256a8bdc8e0fc48c8d5fea1476176643e14ac41a4c2dbce014")
	return nil 
}

func CreateFileMetaMap(db *sql.DB) (map[string]*FileMetaData, error) {
	fileMetaMap := make(map[string]*FileMetaData)

	rows,err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error while reading the distinct filenames from client local indexes.db")
		return fileMetaMap, err
	}

	for rows.Next() {
		var fileName string
		rows.Scan(&fileName)
		temp := new(FileMetaData) 
		fileMetaMap[fileName] = temp

		rowsPerFileName,err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Println("Error while reading the distinct filenames from client local indexes.db")
		}

		var version  int32
		hashlist := []string{}
		for rowsPerFileName.Next(){
			var hashIndex int
			var hashValue string 
			rowsPerFileName.Scan(&fileName, &version, &hashIndex, &hashValue)
			hashlist = append(hashlist, hashValue)
		}
		fileMetaMap[fileName].Filename = fileName
		fileMetaMap[fileName].Version = version
		fileMetaMap[fileName].BlockHashList = hashlist
	}
	return fileMetaMap, nil
}

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	// check if path is correct
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)

	// check if indexes.db is present, if not create one
	err := IndexesDbHandler(metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening indexes.db database on client")
	}

	//creating dummy entries
	// CreateEntriesInDB(metaFilePath)

	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		log.Println("Error in searching for metaFilePath:", e)
		return fileMetaMap, nil
	}

	// open db connection
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening indexes.db database on client")
	}
	defer db.Close()

	// create fileMetaMap by reading from db
	return CreateFileMetaMap(db)
}

func LoadMetaFromMetaDirectory(baseDir string, blockSize int) (fileMetaMap map[string]*FileMetaData, e error) {
	fmt.Println("Loading Meta data from directory")
	// check if path is correct
	fileMetaMap = make(map[string]*FileMetaData)
	f,_ := filepath.Abs(baseDir)
	baseDirFiles, _ := ioutil.ReadDir(f)

	for _, file := range baseDirFiles {
		fileName := file.Name() 
		if !ValidFileName(fileName) {
			continue
		}
		fileMetaMap[fileName] = new(FileMetaData) 
		fileMetaMap[fileName].Filename = fileName
		fileMetaMap[fileName].Version = 1
		reader, _ := os.Open(baseDir + "/" + fileName)
		fileSize := file.Size()
		blocks := int(math.Ceil((float64(1.0*fileSize)/float64(blockSize))))

		blockHashList := []string{}

		for i:=0; i<blocks; i++ {
			data := make([]byte, blockSize)
			bytesRead, _ := reader.Read(data)
			finaldata := data[:bytesRead]
			hash := GetBlockHashString(finaldata)
			blockHashList = append(blockHashList, hash)
		}
		fileMetaMap[fileName].BlockHashList = blockHashList
	}
	return fileMetaMap, nil
}

func IsDeleted(metaData *FileMetaData) bool {
	return len(metaData.BlockHashList) == 1 && metaData.BlockHashList[0] == "0"
}

func ClientFileDownload(serverMetaData *FileMetaData,  clientMetaData *FileMetaData, filePath string, client RPCClient) error {
	var blockStoreAddrs []string
	
	err := client.GetBlockStoreAddrs(&blockStoreAddrs)
	
	if err != nil {
		log.Println("Error while getting block store address", err)
		return err
	}

	// create a new file 
	newfile, err := os.Create(filePath)
    if err != nil {
        log.Println("Unable to create new file: ", err)
		return err
    }
    defer newfile.Close()
	blockHashesIn := serverMetaData.BlockHashList
	*clientMetaData = *serverMetaData

	var blockStoreMap map[string][]string
	log.Println("Fetching the servers for each block hash")
	if err := client.GetBlockStoreMap(blockHashesIn, &blockStoreMap); err != nil {
		log.Println("Error while getting block store map", err)
		return err
	}

	log.Println("Fetched the servers for each block hash->", blockStoreMap)

	// if file is deleted
	if IsDeleted(clientMetaData) {
		err = os.Remove(filePath)
		if err != nil {
			log.Println("Error deleting file:", err)
			return err
		}
		log.Println("File deleted successfully.")	
		return nil
	}
	
	log.Println("Downloading file: ", serverMetaData.BlockHashList)
	writeToFileData := ""

	metaDataAndBlockMap := make(map[string]Block)

	for server, hashes := range blockStoreMap{
		for _, hash := range hashes {
			var metaDataBlock Block
			if err := client.GetBlock(hash, server, &metaDataBlock); err != nil{
				log.Println("Error while getting block: ", err)
				return err
			}
			metaDataAndBlockMap[hash] = metaDataBlock
		}
	}

	for _, metaDataHash := range serverMetaData.BlockHashList {
		writeToFileData += string(metaDataAndBlockMap[metaDataHash].BlockData)
	}
	newfile.WriteString(writeToFileData)
	return nil
}

func ValidFileName(fileName string) bool {
	if(fileName == DEFAULT_META_FILENAME || strings.ContainsAny(fileName, ",/")){
		return false
	}
	return true
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
