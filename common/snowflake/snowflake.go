package snowflake

import (
	"errors"
	"sync"
	"time"
)

const (
	workerIdBits  = 10
	sequenceBits  = 12
	maxWorkerId   = -1 ^ (-1 << workerIdBits)
	maxSequence   = -1 ^ (-1 << sequenceBits)
	timeShift     = workerIdBits + sequenceBits
	workerIdShift = sequenceBits
	epoch         = 1672531200000
)

type Snowflake struct {
	mu        sync.Mutex
	timestamp int64
	workerId  int64
	sequence  int64
}

func New(workerId int64) (*Snowflake, error) {
	if workerId < 0 || workerId > maxWorkerId {
		return nil, errors.New("invalid worker id")
	}
	return &Snowflake{workerId: workerId}, nil
}

func (s *Snowflake) Generate() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UnixMilli()

	if now == s.timestamp {
		s.sequence = (s.sequence + 1) & maxSequence
		if s.sequence == 0 {
			for now <= s.timestamp {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		s.sequence = 0
	}

	s.timestamp = now

	return (now-epoch)<<timeShift | (s.workerId << workerIdShift) | s.sequence
}
