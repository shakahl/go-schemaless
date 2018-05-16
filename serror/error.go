// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2018 Ryan Bastic. All rights reserved.
// This class is derived from golang.org/src/encoding/gob/error.go
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package serror

import "fmt"

const (
	prefix = "schemaless: "
)

// A SchemalessError is used to distinguish errors (panics) generated from Schemaless
// behavior rather than system behavior itself.
type SchemalessError struct {
	err error
}

// Errorf is like error_ but takes Printf-style arguments to construct an error.
// It always prefixes the message with "schemaless: ".
func Errorf(format string, args ...interface{}) {
	Panic(fmt.Errorf(prefix+format, args...))
}

// Error wraps the argument error and uses it as the argument to panic.
func Panic(err error) {
	panic(SchemalessError{err})
}

// CatchError is meant to be used as a deferred function to turn a panic(SchemalessError) into a
// plain error. It overwrites the error return of the function that deferred its call.
func CatchError(err *error) {
	if e := recover(); e != nil {
		se, ok := e.(SchemalessError)
		if !ok {
			panic(e)
		}
		*err = se.err
	}
}
