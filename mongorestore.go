package main

import (
	"fmt"

	commonopts "github.com/mongodb/mongo-tools-common/options"
	mr "github.com/mongodb/mongo-tools/mongorestore"
)

// getMongoRestore returns an initialized MongoRestore object.
func getMongoRestore(dbInfo *DatabaseInfo, targetDir string) (*mr.MongoRestore, error) {
	opts, err := mr.ParseOptions(dbInfo.args, "", "")
	if err != nil {
		return nil, err
	}

	connection := &commonopts.Connection{
		Host: dbInfo.targetHost,
		Port: dbInfo.port,
	}

	opts.ToolOptions = &commonopts.ToolOptions{
		Connection: connection,
		Auth: &commonopts.Auth{
			Username: "",
			Password: "",
		},
	}

	restore, err := mr.New(opts)
	if err != nil {
		return nil, err
	}
	restore.TargetDirectory = targetDir
	restore.NSOptions.DB = dbInfo.targetDB

	return restore, nil
}

// initAndRestore initializes a MongoRestore Object and restores collections.
func initAndRestore(dbInfo *DatabaseInfo, targetDir string) error {
	restore, err := getMongoRestore(dbInfo, targetDir)
	if err != nil {
		fmt.Printf("mongo restore initialization failed: %s", err)
		return err
	}
	if result := restore.Restore(); result.Err != nil {
		fmt.Printf("mongo restore failed: %s", result.Err)
		return result.Err
	}
	return nil
}

// executeMongoRestore processes a restore operation.
func executeMongoRestore(dbInfo *DatabaseInfo) error {
	if len(dbInfo.collections) == 0 {
		targetDir := dbInfo.dumpDir + "/" + dbInfo.sourceDB
		if err := initAndRestore(dbInfo, targetDir); err != nil {
			return err
		}
	} else {
		for _, col := range dbInfo.collections {
			targetDir := dbInfo.dumpDir + "/" + dbInfo.sourceDB + "/" + col + ".bson"
			if err := initAndRestore(dbInfo, targetDir); err != nil {
				return err
			}
		}
	}
	//if err := assertDatabaseConsistency(dbInfo, "target"); err != nil {
	//	return err
	//}
	return nil
}
