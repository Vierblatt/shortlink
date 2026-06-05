package bloom

import (
	"context"
	"hash/fnv"

	"github.com/go-redis/redis/v8"
)

type BloomFilter struct {
	redisClient *redis.Client
	key         string
	size        int64
	hashFuncs   int
}

func New(redisClient *redis.Client, key string, size int64, hashFuncs int) *BloomFilter {
	return &BloomFilter{
		redisClient: redisClient,
		key:         key,
		size:        size,
		hashFuncs:   hashFuncs,
	}
}

func (bf *BloomFilter) Add(data []byte) error {
	for i := 0; i < bf.hashFuncs; i++ {
		hash := fnv.New32a()
		hash.Write(data)
		hash.Write([]byte{byte(i)})
		index := hash.Sum32() % uint32(bf.size)
		if err := bf.redisClient.SetBit(context.Background(), bf.key, int64(index), 1).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (bf *BloomFilter) Test(data []byte) (bool, error) {
	for i := 0; i < bf.hashFuncs; i++ {
		hash := fnv.New32a()
		hash.Write(data)
		hash.Write([]byte{byte(i)})
		index := hash.Sum32() % uint32(bf.size)
		bit, err := bf.redisClient.GetBit(context.Background(), bf.key, int64(index)).Result()
		if err != nil {
			return false, err
		}
		if bit == 0 {
			return false, nil
		}
	}
	return true, nil
}
