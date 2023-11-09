package main

import (
	"bytes"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
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

func accountSnapshotKey(hash common.Hash) []byte {
	return append(SnapshotAccountPrefix, hash.Bytes()...)
}

func main() {
	count = make(map[string]int)
	originDB, err := leveldb.OpenFile("./validator_3/chaindata", &opt.Options{ReadOnly: false})
	if err != nil {
		panic(err)
	}
	defer originDB.Close()
	log.Println("open originDB success")

	targetDB, err := leveldb.OpenFile("./gethdata/geth/chaindata", nil)
	if err != nil {
		panic(err)
	}
	defer targetDB.Close()
	log.Println("open targetDB success")

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

type Account struct {
	Nonce    uint64
	Balance  *big.Int
	Root     []byte
	CodeHash []byte
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
		account := new(Account)
		if err := rlp.DecodeBytes(value, account); err != nil {
			panic(err)
		}
		fmt.Println("-------------------")
		fmt.Println(common.BytesToHash(key[len(SnapshotAccountPrefix):]))
		fmt.Println(common.BytesToHash(account.Root))
		fmt.Println(common.BytesToHash(account.CodeHash))
		fmt.Println(account.Balance)
		fmt.Println(account.Nonce)
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
