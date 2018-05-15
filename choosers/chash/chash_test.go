package chash

import (
	"github.com/rbastic/go-schemaless"
)

var _ schemaless.Chooser = &CHash{}
