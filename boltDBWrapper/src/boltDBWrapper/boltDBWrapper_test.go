package boltDBWrapper

import (
	"testing"
	"time"
)

func TestCreateBoltDB(t *testing.T) {

	expeced := "test"

	cache := CreateKVS(expeced)

	if cache.Table != expeced {
		t.Error("Table name not correct")
	}
}

func TestPut(t *testing.T) {

	key := "spam"
	val := "ham"

	cache := CreateKVS("test2")

	cache.SetExpireTime(1)

	cache.Put(key, val)

	time.Sleep(2 * time.Second)

	if cache.Get(key) == val {
		t.Error("#2 Not expire")
	}

	cache.Delete(key)

	// # Never expired
	cache.SetExpireTime(0)

	cache.Put(key, val)
	time.Sleep(1 * time.Second)

	out := cache.Get(key)
	if out != val {
		t.Error("#3 missing key: " + out)
	}

}
