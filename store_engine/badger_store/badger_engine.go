package badger_store

import (
	"github.com/dgraph-io/badger/v3"
	"pixiu-dkv-go/dkv_conf"
	"pixiu-dkv-go/store_engine"
)

var (
	gKvBadgerEngine *KvBadgerEngine = &KvBadgerEngine{nil}
)

type KvBadgerEngine struct {
	db *badger.DB
}

func GetKvStoreEngine() *KvBadgerEngine {
	return gKvBadgerEngine
}

func GetKvStoreLocalDb() *badger.DB {
	return gKvBadgerEngine.db
}

func (kv *KvBadgerEngine) InitStoreEngine() {
	var path = dkv_conf.GetAppConf().KvDataPath
	opts := badger.DefaultOptions(path)
	bdb, err := badger.Open(opts)
	if err != nil {
		return
	}

	kv.db = bdb
}

func (kv *KvBadgerEngine) RawGet(key []byte) ([]byte, error) {
	var item *badger.Item = nil
	err := kv.db.View(func(txn *badger.Txn) error {
		inner, er := txn.Get(key)
		item = inner
		return er
	})

	if err != nil {
		return nil, err
	} else {
		var valCopy []byte
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})

		if err != nil {
			return nil, err
		} else {
			return valCopy, nil
		}
	}
}

func (kv *KvBadgerEngine) RawPut(key []byte, value []byte) error {
	if len(key) < 1 {
		return store_engine.ErrEmptyKey
	}

	err := kv.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})

	return err
}

func (kv *KvBadgerEngine) RawPutBatch(values [][]byte) error {
	txn := kv.db.NewTransaction(true)
	for i := 0; i < len(values); {
		key := values[i]
		value := values[i+1]

		oerr := txn.Set(key, value)
		if oerr != nil {
			if oerr == badger.ErrTxnTooBig {
				cerr := txn.Commit()
				if cerr != nil {
					return cerr
				}

				txn = kv.db.NewTransaction(true)
				cerr = txn.Set(key, value)
				if cerr == nil {
					i = i + 2
				} else {
					return cerr
				}
			} else {
				return oerr
			}
		} else {
			i = i + 2
		}
	}

	return txn.Commit()
}

func (kv *KvBadgerEngine) RawDel(key []byte) error {
	if len(key) < 1 {
		return store_engine.ErrEmptyKey
	}

	return kv.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		return err
	})
}

func (kv *KvBadgerEngine) RawDelBatch(keys [][]byte) error {
	txn := kv.db.NewTransaction(true)
	for i := 0; i < len(keys); {
		tmpKeyBuf := keys[i]
		oerr := txn.Delete(tmpKeyBuf)
		if oerr != nil {
			if oerr == badger.ErrTxnTooBig {
				cerr := txn.Commit()
				if cerr != nil {
					return cerr
				}

				txn = kv.db.NewTransaction(true)
				cerr = txn.Delete(tmpKeyBuf)
				if cerr != nil {
					return cerr
				} else {
					i = i + 2
				}
			} else {
				return oerr
			}
		} else {
			i = i + 2
		}
	}

	return txn.Commit()
}
