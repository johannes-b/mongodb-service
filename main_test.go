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

// setEnvironmentVariables is a helper method to set various environment
// variables needed for running the tests
func setEnvironmentVariables() {
	//configuration for carts service
	os.Setenv("CARTS_SOURCEDB", "carts-db")
	os.Setenv("CARTS_TARGETDB", "carts-db-test")
	os.Setenv("CARTS_PORT", "27017")
	os.Setenv("CARTS_HOST", "")
	os.Setenv("CARTS_COLLECTIONS", "")

	//additional env variables for test cases
	os.Setenv("DUMP_DIR_ONE_COLLECTION", "./dumpdir/dumpDirOneCollection")
	os.Setenv("DUMP_DIR_MULTIPLE_COLLECTIONS", "./dumpdir/dumpDirMultipleCollections")
	os.Setenv("DUMP_DIR_ALL_COLLECTIONS", "./dumpdir/dumpDirAllCollections")
	os.Setenv("CARTS_COLLECTIONS_2", "items")
	os.Setenv("CARTS_COLLECTIONS_3", "items;categories")
}

// fail is a helper method to mark the calling test as failed
// and prints the error message
func fail(err error, t *testing.T) {
	fmt.Println(err)
	t.Fail()
}

func TestMain(m *testing.M) {
	fmt.Println("test main")
	setEnvironmentVariables()
	os.Exit(m.Run())
}

// TestMongoDriver instantiates the mongo driver.
func TestMongoDriver(t *testing.T) {
	fmt.Println("\n>> TestMongoDriver()")
	StartTimer()
	
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	dbInfo := &DatabaseInfo{
		sourceDB: os.Getenv("CARTS_SOURCEDB"),
		host:     os.Getenv("CARTS_HOST"),
		port:     os.Getenv("CARTS_PORT"),
	}
	db, err := getDatabase(ctx, dbInfo)
	if err != nil {
		fail(err, t)
	}
	singleResult := db.RunCommand(ctx, bson.M{"listCommands": 1})

	if singleResult.Err() != nil {
		fail(singleResult.Err(), t)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestMongoDumpAllCollections executes mongo dump for all
// the collections in the database.
func TestMongoDumpAllCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpAllCollections()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		host:        os.Getenv("CARTS_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
	}
	if err := executeMongoDump(dbInfo); err != nil {
		fail(err, t)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestMongoDumpOneCollection executes mongo dump for the
// categories collection.
func TestMongoDumpOneCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpOneCollection()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		host:        os.Getenv("CARTS_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ONE_COLLECTION"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_2")),
	}
	if err := executeMongoDump(dbInfo); err != nil {
		fail(err, t)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestMongoDumpMultipleCollections executes mongo dump for the
// multiple collections.
func TestMongoDumpMultipleCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpMultipleCollections()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		host:        os.Getenv("CARTS_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_3")),
	}
	if err := executeMongoDump(dbInfo); err != nil {
		fail(err, t)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestMongoRestoreAllCollections executes mongo restore for all
// the collections in the database.
func TestMongoRestoreAllCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreAllCollections()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		targetDB:    "carts-db-test-1",
		host:        os.Getenv("CARTS_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		fail(err, t)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestMongoRestoreOneCollection executes mongo restore for
// the categories collection.
func TestMongoRestoreOneCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreOneCollection()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		targetDB:    "carts-db-test-2",
		host:        os.Getenv("CARTS_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ONE_COLLECTION"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_2")),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		fail(err, t)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestMongoRestoreMultipleCollection executes mongo restore for
// the categories collection.
func TestMongoRestoreMultipleCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreMultipleCollections()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		targetDB:    "carts-db-test-3",
		host:        os.Getenv("CARTS_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_3")),
		args:        []string{},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		fail(err, t)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestDatabaseSync executes a synchronization of two databases
//(dump and restore operation).
func TestDatabaseSync(t *testing.T) {
	fmt.Println("\n>> TestDatabaseSync()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		targetDB:    "carts-db-test-4",
		host:        os.Getenv("CARTS_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
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
	fmt.Printf("Duration: %s", GetDuration())
}
