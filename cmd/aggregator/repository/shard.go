package repository

import (
	"fmt"
	"strings"
)

const ShardCount = 16

type Shard struct {
	Index int
	Name  string
	Low   string
	High  string
}

func Shards() []Shard {
	shards := make([]Shard, ShardCount)
	for i := range ShardCount {
		shards[i] = Shard{Index: i, Name: ShardName(i), Low: shardBound(i), High: ""}
		if i+1 < ShardCount {
			shards[i].High = shardBound(i + 1)
		}
	}
	return shards
}

func ShardName(i int) string {
	return fmt.Sprintf("shard-%x", i)
}

func ShardOf(hazardID string) int {
	if hazardID == "" {
		return 0
	}
	var i int
	fmt.Sscanf(strings.ToLower(hazardID[:1]), "%x", &i)
	return i
}

func shardBound(i int) string {
	return fmt.Sprintf("%x0000000-0000-0000-0000-000000000000", i)
}
