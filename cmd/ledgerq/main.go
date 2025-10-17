// Package main provides the LedgerQ CLI tool for queue inspection and management.
package main

import (
	"fmt"
	"os"

	"github.com/vnykmshr/ledgerq"
)

func main() {
	fmt.Printf("LedgerQ CLI v%s\n", ledgerq.Version)
	fmt.Println("Queue inspection tool (implementation pending)")
	os.Exit(0)
}
