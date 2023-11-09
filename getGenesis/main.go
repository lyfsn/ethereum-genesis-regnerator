package main

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"log"
	"math/big"
)

// The fields below define the low level database schema prefixing.
var (
	// databaseVersionKey tracks the current database version.
	databaseVersionKey = []byte("DatabaseVersion")

	// headHeaderKey tracks the latest known header's hash.
	headHeaderKey = []byte("LastHeader")

	// headBlockKey tracks the latest known full block's hash.
	headBlockKey = []byte("LastBlock")

	// headFastBlockKey tracks the latest known incomplete block's hash during fast sync.
	headFastBlockKey = []byte("LastFast")

	// lastPivotKey tracks the last pivot block used by fast sync (to reenable on sethead).
	lastPivotKey = []byte("LastPivot")

	// fastTrieProgressKey tracks the number of trie entries imported during fast sync.
	fastTrieProgressKey = []byte("TrieSync")

	// snapshotDisabledKey flags that the snapshot should not be maintained due to initial sync.
	snapshotDisabledKey = []byte("SnapshotDisabled")

	// snapshotRootKey tracks the hash of the last snapshot.
	snapshotRootKey = []byte("SnapshotRoot")

	// snapshotJournalKey tracks the in-memory diff layers across restarts.
	snapshotJournalKey = []byte("SnapshotJournal")

	// snapshotGeneratorKey tracks the snapshot generation marker across restarts.
	snapshotGeneratorKey = []byte("SnapshotGenerator")

	// snapshotRecoveryKey tracks the snapshot recovery marker across restarts.
	snapshotRecoveryKey = []byte("SnapshotRecovery")

	// snapshotSyncStatusKey tracks the snapshot sync status across restarts.
	snapshotSyncStatusKey = []byte("SnapshotSyncStatus")

	// txIndexTailKey tracks the oldest block whose transactions have been indexed.
	txIndexTailKey = []byte("TransactionIndexTail")

	// fastTxLookupLimitKey tracks the transaction lookup limit during fast sync.
	fastTxLookupLimitKey = []byte("FastTransactionLookupLimit")

	//offSet of new updated ancientDB.
	offSetOfCurrentAncientFreezer = []byte("offSetOfCurrentAncientFreezer")

	//offSet of the ancientDB before updated version.
	offSetOfLastAncientFreezer = []byte("offSetOfLastAncientFreezer")

	// badBlockKey tracks the list of bad blocks seen by local
	badBlockKey = []byte("InvalidBlock")

	// uncleanShutdownKey tracks the list of local crashes
	uncleanShutdownKey = []byte("unclean-shutdown") // config prefix for the db

	// Data item prefixes (use single byte to avoid mixing data types, avoid `i`, used for indexes).
	headerPrefix       = []byte("h") // headerPrefix + num (uint64 big endian) + hash -> header
	headerTDSuffix     = []byte("t") // headerPrefix + num (uint64 big endian) + hash + headerTDSuffix -> td
	headerHashSuffix   = []byte("n") // headerPrefix + num (uint64 big endian) + headerHashSuffix -> hash
	headerNumberPrefix = []byte("H") // headerNumberPrefix + hash -> num (uint64 big endian)

	blockBodyPrefix     = []byte("b") // blockBodyPrefix + num (uint64 big endian) + hash -> block body
	blockReceiptsPrefix = []byte("r") // blockReceiptsPrefix + num (uint64 big endian) + hash -> block receipts

	txLookupPrefix        = []byte("l") // txLookupPrefix + hash -> transaction/receipt lookup metadata
	bloomBitsPrefix       = []byte("B") // bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash -> bloom bits
	SnapshotAccountPrefix = []byte("a") // SnapshotAccountPrefix + account hash -> account trie value
	SnapshotStoragePrefix = []byte("o") // SnapshotStoragePrefix + account hash + storage hash -> storage trie value
	CodePrefix            = []byte("c") // CodePrefix + code hash -> account code

	// difflayer database
	diffLayerPrefix = []byte("d") // diffLayerPrefix + hash  -> diffLayer

	preimagePrefix = []byte("secure-key-")      // preimagePrefix + hash -> preimage
	configPrefix   = []byte("ethereum-config-") // config prefix for the db

	// Chain index prefixes (use `i` + single byte to avoid mixing data types).
	BloomBitsIndexPrefix = []byte("iB") // BloomBitsIndexPrefix is the data table of a chain indexer to track its progress

	preimageCounter    = metrics.NewRegisteredCounter("db/preimage/total", nil)
	preimageHitCounter = metrics.NewRegisteredCounter("db/preimage/hits", nil)
)

var count map[string]int

type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     common.Hash // merkle root of the storage trie
	CodeHash []byte
}
type Storage map[common.Hash]common.Hash
type Code []byte

var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")
var emptyCode = crypto.Keccak256(nil)

func accountSnapshotKey(hash common.Hash) []byte {
	return append(SnapshotAccountPrefix, hash.Bytes()...)
}

func storageSnapshotKey(accountHash, storageHash common.Hash) []byte {
	return append(append(SnapshotStoragePrefix, accountHash.Bytes()...), storageHash.Bytes()...)
}

func storageSnapshotsKey(accountHash common.Hash) []byte {
	return append(SnapshotStoragePrefix, accountHash.Bytes()...)
}

func codeKey(hash common.Hash) []byte {
	return append(CodePrefix, hash.Bytes()...)
}

func preimageKey(hash common.Hash) []byte {
	return append(preimagePrefix, hash.Bytes()...)
}

func main() {
	count = make(map[string]int)
	originDB, err := leveldb.OpenFile("./validator_3/chaindata", &opt.Options{ReadOnly: false})
	if err != nil {
		panic(err)
	}
	defer originDB.Close()
	log.Println("open originDB success")

	targetDB, err := leveldb.OpenFile("./test", nil)
	if err != nil {
		panic(err)
	}
	defer targetDB.Close()
	log.Println("open targetDB success")

	accH := common.HexToHash("0x092c24c96336801f15bf04b92a1543de30696d4dabf612c9426c9771d4041d81")
	storH := common.HexToHash("0x290decd9548b62a8d60345a988386fc84ba6bc95484008f6362f93160ef3e563")
	key := storageSnapshotKey(accH, storH)
	value, err := originDB.Get(key, nil)
	fmt.Println(value, err, common.BytesToHash(value))

	//t2 := common.HexToHash("0x52df0bdf5a5f92d8037cf11e50f13d8017aefc99d20a73c826416df79570d481")
	t3 := common.HexToHash("0x092c24c96336801f15bf04b92a1543de30696d4dabf612c9426c9771d4041d81")
	i := preimageKey(t3)
	value, err = originDB.Get(i, nil)
	fmt.Println(value, err, common.BytesToHash(value), common.BytesToAddress(value))
	return

	iter := originDB.NewIterator(nil, nil)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		handleKey(originDB, targetDB, key, value)
	}

	for k, v := range count {
		log.Printf("key: %s, count: %d", k, v)
	}
}

func handleKey(originDB, targetDB *leveldb.DB, key []byte, value []byte) {
	switch {
	case bytes.HasPrefix(key, headerPrefix) && len(key) == (len(headerPrefix)+8+common.HashLength):
		count["header"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, blockBodyPrefix) && len(key) == (len(blockBodyPrefix)+8+common.HashLength):
		count["blockBody"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, blockReceiptsPrefix) && len(key) == (len(blockReceiptsPrefix)+8+common.HashLength):
		count["blockReceipts"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerTDSuffix):
		count["headerTD"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, headerPrefix) && bytes.HasSuffix(key, headerHashSuffix):
		count["headerHash"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, headerNumberPrefix) && len(key) == (len(headerNumberPrefix)+common.HashLength):
		count["headerNumber"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case len(key) == common.HashLength:
		count["hash"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, CodePrefix) && len(key) == len(CodePrefix)+common.HashLength:
		count["code"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, txLookupPrefix) && len(key) == (len(txLookupPrefix)+common.HashLength):
		count["txLookup"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, SnapshotAccountPrefix) && len(key) == (len(SnapshotAccountPrefix)+common.HashLength):
		count["SnapshotAccount"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, SnapshotStoragePrefix) && len(key) == (len(SnapshotStoragePrefix)+2*common.HashLength):
		count["SnapshotStorage"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, preimagePrefix) && len(key) == (len(preimagePrefix)+common.HashLength):
		count["preimage"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
		address := common.BytesToAddress(value)
		hasher := crypto.NewKeccakState()
		h := crypto.HashData(hasher, address.Bytes())
		snapshotKey := accountSnapshotKey(h)

		accData, err := originDB.Get(snapshotKey, nil)
		if err != nil {
			fmt.Errorf("get account error: %s", err)
		} else {
			account := new(Account)
			err = rlp.DecodeBytes(accData, account)
			if err != nil {
				fmt.Errorf("get account error: %s", err)
			} else {
				fmt.Println(address, account.Balance, account.Nonce, account.Root, account.Root == emptyRoot, bytes.Equal(account.CodeHash, emptyCode), common.BytesToHash(account.CodeHash))

				if !bytes.Equal(account.CodeHash, emptyCode) {
					i := codeKey(common.BytesToHash(account.CodeHash))
					codeData, err := originDB.Get(i, nil)
					if err != nil {
						fmt.Errorf("get code error: %s", err, codeData)
					}
				}
			}
		}

		storageKey := storageSnapshotsKey(h)
		storData, err := originDB.Get(storageKey, nil)
		if len(storData) != 0 {
			storage := make(Storage)
			err = rlp.DecodeBytes(storData, &storage)
			if err != nil {
				fmt.Errorf("get storage error: %s", err)
				return
			} else {
				for k, v := range storage {
					fmt.Println(k, v)
				}
			}
		}

	case bytes.HasPrefix(key, bloomBitsPrefix) && len(key) == (len(bloomBitsPrefix)+10+common.HashLength):
		count["bloomBits"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, BloomBitsIndexPrefix):
		count["BloomBitsIndex"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, []byte("clique-")) && len(key) == 7+common.HashLength:
		count["clique"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, []byte("parlia-")) && len(key) == 7+common.HashLength:
		count["parlia"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, []byte("cht-")) ||
		bytes.HasPrefix(key, []byte("chtIndexV2-")) ||
		bytes.HasPrefix(key, []byte("chtRootV2-")): // Canonical hash trie
		count["cht"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.HasPrefix(key, []byte("blt-")) ||
		bytes.HasPrefix(key, []byte("bltIndex-")) ||
		bytes.HasPrefix(key, []byte("bltRoot-")): // Bloomtrie sub
		count["blt"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	case bytes.Equal(key, uncleanShutdownKey):
		count["uncleanShutdown"]++
		//err := targetDB.Put(key, value, nil)
		//if err != nil {
		//	panic(err)
		//}
	default:
		var accounted bool
		for _, meta := range [][]byte{
			databaseVersionKey, headHeaderKey, headBlockKey, headFastBlockKey, lastPivotKey,
			fastTrieProgressKey, snapshotDisabledKey, snapshotRootKey, snapshotJournalKey,
			snapshotGeneratorKey, snapshotRecoveryKey, txIndexTailKey, fastTxLookupLimitKey,
			uncleanShutdownKey, badBlockKey,
		} {
			if bytes.Equal(key, meta) {
				accounted = true
				count["accounted"]++
				break
			}
		}
		if !accounted {
			count["unknown"]++
		}
	}
}
