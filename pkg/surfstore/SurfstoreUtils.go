package surfstore

import (
	"io/ioutil"
	"log"
	"math"
	"os"
	"reflect"
	//"golang.org/x/text/unicode/rangetable"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println("Error when reading basedir: ", err)
	}

	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	//debug
	//PrintMetaMap(localIndex)
	if err != nil {
		log.Println("cannot read from metafile", err)
	}

	//Sync local index
	curMap := make(map[string][]string)
	for _, file := range files {
		if file.Name() == "index.db" {
			continue
		}
		var number int = int(math.Ceil(float64(file.Size()) / float64(client.BlockSize)))
		//to read the file and get the hash index
		curFile, err := os.Open(client.BaseDir + "/" + file.Name())
		if err != nil {
			log.Println("Error reading file in basedir: ", err)
		}

		for i := 0; i < number; i++ {
			newBlock := make([]byte, client.BlockSize)
			len, err := curFile.Read(newBlock)
			if err != nil {
				log.Println("error in reading block from file: ", err)
			}
			newBlock = newBlock[:len]
			hash := GetBlockHashString(newBlock)
			curMap[file.Name()] = append(curMap[file.Name()], hash)
		}

		if val, ok := localIndex[file.Name()]; ok {
			if !reflect.DeepEqual(curMap[file.Name()], val.BlockHashList) { //TODO: Works??
				localIndex[file.Name()].BlockHashList = curMap[file.Name()]
				localIndex[file.Name()].Version++
			}
		} else {
			// New file
			newFileMeta := FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: curMap[file.Name()]}
			localIndex[file.Name()] = &newFileMeta
		}
	}

	//Check for deleted files
	for fileName, metaData := range localIndex {
		if _, ok := curMap[fileName]; !ok {
			if len(metaData.BlockHashList) != 1 || metaData.BlockHashList[0] != "0" {
				metaData.Version++
				metaData.BlockHashList = []string{"0"}
			}
		}
	}
	//get the remote Index for files
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		log.Println("cannot get the remoteIndex")
	}
	//get the block address
	var address string
	err = client.GetBlockStoreAddr(&address)
	if err != nil {
		log.Println("cannot get the blockstoreaddr: ", err)
	}
	//compare the localIndex with the remoteIndex
	for fileName, localMetaData := range localIndex {
		// remoteIndex don't have this file, just upload it
		if _, ok := remoteIndex[fileName]; !ok {
			uploadFile(client, fileName, localIndex, address)
		} else {
			//check for the version and upload
			if localMetaData.Version > remoteIndex[fileName].Version {
				uploadFile(client, fileName, localIndex, address)
			}
		}
	}
	// if remote index have files that local don't have, just download it
	for fileName, remoteMetaData := range remoteIndex {
		if _, ok := localIndex[fileName]; !ok {
			downloadFile(client, localIndex, remoteMetaData, address)
		} else {
			localMetaData := localIndex[fileName]
			if localMetaData.Version < remoteMetaData.Version {
				downloadFile(client, localIndex, remoteMetaData, address)
			} else if localMetaData.Version == remoteMetaData.Version {
				if !reflect.DeepEqual(localMetaData.BlockHashList, remoteMetaData.BlockHashList) {
					downloadFile(client, localIndex, remoteMetaData, address)
				}
			}
		}
	}
	//debug
	//PrintMetaMap(localIndex)
	WriteMetaFile(localIndex, client.BaseDir)
}

func uploadFile(client RPCClient, fileName string, localIndex map[string]*FileMetaData, address string) error {
	//1. update the remoteIndex for the file
	//2. get the current file by the filename
	//3. read the fileBlocks, update the fileBlocks in the BlockStore
	latestVersion := &Version{}
	fileMetaData := localIndex[fileName]
	client.UpdateFile(fileMetaData, &latestVersion.Version)
	// if curVersion is outdated syn the current data with the remote data
	if latestVersion.Version == -1 {
		log.Println("version is outdated")
		remoteIndex := make(map[string]*FileMetaData)
		client.GetFileInfoMap(&remoteIndex)
		err := downloadFile(client, localIndex, remoteIndex[fileName], address)
		if err != nil {
			log.Println("downloadFile error:", err)
			return err
		}
		return nil
	}
	// else just update blockStore but if the update is deleted file
	path := client.BaseDir + "/" + fileName
	if len(fileMetaData.BlockHashList) == 1 && fileMetaData.BlockHashList[0] == "0" {
		return nil
	}
	curFile, err := os.Open(path)
	defer curFile.Close()
	if err != nil {
		log.Println("Error reading file in basedir: ", err)
	}

	for i := 0; i < len(fileMetaData.BlockHashList); i++ {
		newBlock := make([]byte, client.BlockSize)
		len, err := curFile.Read(newBlock)
		if err != nil {
			log.Println("error in reading block from file: ", err)
		}
		newBlock = newBlock[:len]
		var succ bool
		curBlock := &Block{BlockData: newBlock, BlockSize: int32(len)}
		client.PutBlock(curBlock, address, &succ)
		if succ == false {
			log.Println("error in put block to remote:")
			return nil
		}
	}
	return nil
}

func downloadFile(client RPCClient, localIndex map[string]*FileMetaData, remoteMetaData *FileMetaData, address string) error {
	//1. use the remote metadata to update local metaData
	//2. write the remote blocks into the current file
	remoteFileName := remoteMetaData.Filename
	path := client.BaseDir + "/" + remoteFileName
	localIndex[remoteFileName] = remoteMetaData
	file, err := os.Create(path)
	if err != nil {
		log.Println("cannot create the file:", err)
	}
	defer file.Close()
	if len(remoteMetaData.BlockHashList) == 1 && remoteMetaData.BlockHashList[0] == "0" {
		err = os.Remove(path)
		if err != nil {
			log.Println("error in remove local file:", err)
		}
		return nil
	}
	for _, hash := range remoteMetaData.BlockHashList {
		var block Block
		err = client.GetBlock(hash, address, &block)
		if err != nil {
			log.Println("Fail to get the block")
		}
		file.Write(block.BlockData)
	}
	return nil
}
