package main

import (
	"fmt"

	commonopts "github.com/mongodb/mongo-tools-common/options"
	md "github.com/mongodb/mongo-tools/mongodump"
)

// getMongoDump returns an initialized MongoDump object.
func getMongoDump(dbInfo *DatabaseInfo) *md.MongoDump {
	connection := &commonopts.Connection{
		Host: dbInfo.sourceHost,
		Port: dbInfo.sourcePort,
	}

	toolOptions := &commonopts.ToolOptions{
		Connection: connection,
		Namespace:  &commonopts.Namespace{DB: dbInfo.sourceDB},
		Auth: &commonopts.Auth{
			Username: "",
			Password: "",
		},
		URI: &commonopts.URI{},
	}

	inputOptions := &md.InputOptions{}
	outputOptions := &md.OutputOptions{
		NumParallelCollections: 1,
		Out:                    dbInfo.dumpDir,
	}

	return &md.MongoDump{
		ToolOptions:   toolOptions,
		InputOptions:  inputOptions,
		OutputOptions: outputOptions,
	}
}

// initAndDump initializes a MongoDump Object and restores collections.
func initAndDump(dbInfo *DatabaseInfo, col string) error {
	mongoDump := getMongoDump(dbInfo)
	mongoDump.ToolOptions.Collection = col

	if err := mongoDump.Init(); err != nil {
		fmt.Printf("mongo dump initialization failed: %s", err)
		return err
	}
	if err := mongoDump.Dump(); err != nil {
		fmt.Printf("mongo dump failed: %s", err)
		return err
	}
	return nil
}

// executeMongoDump processes a mongodump operation.
func executeMongoDump(dbInfo *DatabaseInfo) error {
	if len(dbInfo.collections) == 0 { //dump all collections
		if err := initAndDump(dbInfo, ""); err != nil {
			return err
		}
	} else {
		for _, col := range dbInfo.collections {
			if err := initAndDump(dbInfo, col); err != nil {
				return err
			}
		}
	}
	fmt.Println ("Dumped database successfully. Starting with file check...")
	if err := assertDatabaseConsistency(dbInfo, "source"); err != nil {
		return err
	}
	return nil
}
