package boltDBWrapper

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

// CreateKVS is alias for CreateBoltDB
var CreateKVS = CreateBoltDB

// DefaultExpireTime is default value for expire time
// If you change the time, use SetExpireTime function
var DefaultExpireTime = int64(0)

type BoltDB struct {
	Path   string
	DB     *bolt.DB
	Table  string
	Expire int64
}

func CreateBoltDB(name string) *BoltDB {

	cache := BoltDB{
		Path:   "my.db",
		Table:  name,
		Expire: DefaultExpireTime,
	}

	db, err := bolt.Open(cache.Path, 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	cache.DB = db

	cache.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(cache.Table))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})

	return &cache
}

func (c *BoltDB) GetExpireTime() int64 {
	return c.Expire
}

// SetExpireTime is used to set expire time by second
// If value is 0, never expired
func (c *BoltDB) SetExpireTime(e int64) error {
	c.Expire = e
	return nil
}

func (c *BoltDB) Put(key string, value string) error {

	now := time.Now().Unix()
	v := strconv.FormatInt(now, 10) + ":" + value

	c.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(c.Table))
		err := b.Put([]byte(key), []byte(v))
		return err
	})
	return nil
}

func (c *BoltDB) Get(key string) string {

	var ret string

	c.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(c.Table))
		v := b.Get([]byte(key))
		ret = string(v)
		return nil
	})

	splitted := strings.SplitN(ret, ":", 2)
	expire := c.GetExpireTime()

	if expire == 0 {
		return splitted[1]
	}

	storedTime, _ := strconv.ParseInt(splitted[0], 10, 64)
	now := time.Now().Unix()

	if now-storedTime > expire {
		c.Delete(key)
		return ""
	}

	return splitted[1]
}

func (c *BoltDB) Delete(key string) error {

	err := c.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(c.Table))
		err := b.Delete([]byte(key))
		return err
	})

	return err

}
