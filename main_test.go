// For the tests a mongo database named "carts-db" was created
//containing the collections items, categories and users.
package main

import (
	"context"
	"fmt"
	"os"
	"testing"

	mr "github.com/mongodb/mongo-tools/mongorestore"
	"gopkg.in/mgo.v2/bson"
)

func setEnvironmentVariables(t *testing.T) {
	os.Setenv("DUMP_DIR_ONE_COLLECTION", "./dumpdir/dumpDirOneCollection")
	os.Setenv("DUMP_DIR_MULTIPLE_COLLECTIONS", "./dumpdir/dumpDirMultipleCollections")
	os.Setenv("DUMP_DIR_ALL_COLLECTIONS", "./dumpdir/dumpDirAllCollections")
}

// TestMongoDriver instantiates the mongo driver.
func TestMongoDriver(t *testing.T) {
	fmt.Println("\n>> TestMongoDriver()")

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	dbInfo := &DatabaseInfo{
		sourceDB: cartsDB,
		host:     defaultHost,
		port:     defaultPort,
	}
	db, err := getDatabase(ctx, dbInfo)
	if err != nil {
		fail(err, t)
	}
	singleResult := db.RunCommand(ctx, bson.M{"listCommands": 1})

	if singleResult.Err() != nil {
		fail(singleResult.Err(), t)
	}
}

// TestMongoDumpAllCollections executes mongo dump for all
// the collections in the database.
func TestMongoDumpAllCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpAllCollections()")

	setEnvironmentVariables(t)

	dbInfo := &DatabaseInfo{
		sourceDB:    cartsDB,
		host:        defaultHost,
		port:        defaultPort,
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: []string{},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		fail(err, t)
	}
}

// TestMongoDumpOneCollection executes mongo dump for the
// categories collection.
func TestMongoDumpOneCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpOneCollection()")

	setEnvironmentVariables(t)

	dbInfo := &DatabaseInfo{
		sourceDB: cartsDB,
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  os.Getenv("DUMP_DIR_ONE_COLLECTION"),
		collections: []string{
			itemsCol,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		fail(err, t)
	}
}

// TestMongoDumpMultipleCollections executes mongo dump for the
// multiple collections.
func TestMongoDumpMultipleCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpMultipleCollections()")

	setEnvironmentVariables(t)

	dbInfo := &DatabaseInfo{
		sourceDB: cartsDB,
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS"),
		collections: []string{
			itemsCol,
			categoriesCol,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		fail(err, t)
	}
}

// TestMongoRestoreAllCollections executes mongo restore for all
// the collections in the database.
func TestMongoRestoreAllCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreAllCollections()")

	setEnvironmentVariables(t)

	dbInfo := &DatabaseInfo{
		sourceDB:    cartsDB,
		targetDB:    "carts-db-test-1",
		host:        defaultHost,
		port:        defaultPort,
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: []string{},
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		fail(err, t)
	}
}

// TestMongoRestoreOneCollection executes mongo restore for
// the categories collection.
func TestMongoRestoreOneCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreOneCollection()")

	setEnvironmentVariables(t)

	dbInfo := &DatabaseInfo{
		sourceDB: cartsDB,
		targetDB: "carts-db-test-2",
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  os.Getenv("DUMP_DIR_ONE_COLLECTION"),
		collections: []string{
			itemsCol,
		},
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		fail(err, t)
	}
}

// TestMongoRestoreMultipleCollection executes mongo restore for
// the categories collection.
func TestMongoRestoreMultipleCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreMultipleCollections()")

	setEnvironmentVariables(t)

	dbInfo := &DatabaseInfo{
		sourceDB: cartsDB,
		targetDB: "carts-db-test-3",
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS"),
		collections: []string{
			itemsCol,
			categoriesCol,
		},
		args: []string{},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		fail(err, t)
	}
}

// TestSync executes a synchronization of two databases
//(dump and restore operation).
func TestDatabaseSync(t *testing.T) {
	fmt.Println("\n>> TestDatabaseSync()")

	setEnvironmentVariables(t)

	dbInfo := &DatabaseInfo{
		sourceDB:    cartsDB,
		targetDB:    "carts-db-test-4",
		host:        defaultHost,
		port:        defaultPort,
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: []string{},
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		fail(err, t)
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		fail(err, t)
	}
}
