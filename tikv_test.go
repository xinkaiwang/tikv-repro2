package main

import (
	"context"
	// "flag"
	"encoding/hex"
	// "fmt"
	// "os"
	"testing"

	"github.com/tikv/client-go/config"
	// "github.com/tikv/client-go/key"
	"github.com/tikv/client-go/txnkv"
	"github.com/stretchr/testify/assert"
)

func OpenKvStorage() (*txnkv.Client, error) {
	client, err := txnkv.NewClient(context.TODO(), []string{"127.0.0.1:2379"}, config.Default())
	return client, err
}

func TestBasicGet(t *testing.T) {
	storage, err := OpenKvStorage()
	assert.Nil(t, err)
	assert.NotNil(t, storage)

	rawKeyStr := "74800000080000006c5f69662eff313031fb"
	rawValueStr := "06f80306000000000031"

	rawKey, err := hex.DecodeString(rawKeyStr)
	assert.Nil(t, err)
	rawValue, err := hex.DecodeString(rawValueStr)
	assert.Nil(t, err)

	{
		// delete
		tx, err := storage.Begin(context.Background())
		assert.Nil(t, err)
		defer tx.Rollback()
		err = tx.Delete(rawKey)
		assert.Nil(t, err)
		err = tx.Commit(context.Background())
		assert.Nil(t, err)
	}

	{
		// write
		tx, err := storage.Begin(context.Background())
		assert.Nil(t, err)
		defer tx.Rollback()
		err = tx.Set(rawKey, rawValue)
		assert.Nil(t, err)
		err = tx.Commit(context.Background())
		assert.Nil(t, err)
	}

	{
		// read (verify)
		tx, err := storage.Begin(context.Background())
		assert.Nil(t, err)
		defer tx.Rollback()
		result, err := tx.Get(context.Background(), rawKey)
		assert.Nil(t, err)
		assert.Equal(t, rawValueStr, hex.EncodeToString(result))
	}
}