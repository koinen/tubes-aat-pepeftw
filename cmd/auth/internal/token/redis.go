package token

import (
	_ "embed"

	"github.com/redis/go-redis/v9"
)

//go:embed lua/script.lua
var tokenLuaScript string

var rotate = redis.NewScript(tokenLuaScript)
