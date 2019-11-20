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
// variables needed for running the tests.
func setEnvironmentVariables() {
	//configuration for carts service
	os.Setenv("CARTS_SOURCEDB", "carts-db") //84.0KB
	os.Setenv("CARTS_TARGETDB", "carts-db-canary")
	os.Setenv("CARTS_PORT", "27017")
	os.Setenv("CARTS_SOURCE_HOST", "localhost")
	os.Setenv("CARTS_TARGET_HOST", "localhost")
	os.Setenv("CARTS_COLLECTIONS", "")

	os.Setenv("TRADES_SOURCEDB", "trades-db") //1.2MB
	os.Setenv("TRADES_TARGETDB", "trades-db-test")
	os.Setenv("TRADES_PORT", "27017")
	os.Setenv("TRADES_HOST", "")

	//additional env variables for test cases
	os.Setenv("DUMP_DIR_ONE_COLLECTION", "./dumpdir/dumpDirOneCollection")
	os.Setenv("DUMP_DIR_MULTIPLE_COLLECTIONS", "./dumpdir/dumpDirMultipleCollections")
	os.Setenv("DUMP_DIR_ALL_COLLECTIONS", "./dumpdir/dumpDirAllCollections")
	os.Setenv("DUMP_DIR_TRADES_DB", "./dumpdir/dumpDirTradesDb")
	os.Setenv("CARTS_COLLECTIONS_2", "items")
	os.Setenv("CARTS_COLLECTIONS_3", "items;categories")
}

// assertError compares the actual error message with the expected.
func assertError(t *testing.T, expectedMsg string, actualError error) {
	if actualError == nil {
		t.Error("expected an error, but no error was thrown.")
		return
	}
	if actualError.Error() != expectedMsg {
		t.Errorf("unexpected error message, expected: %s, found: %s", expectedMsg, actualError)
	}
}

func TestMain(m *testing.M) {
	setEnvironmentVariables()
	m.Run()
	//os.RemoveAll("./dumpdir/")
}

// TestMongoDriver instantiates the mongo driver.
func TestMongoDriver(t *testing.T) {
	fmt.Println("\n>> TestMongoDriver()")
	StartTimer()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	dbInfo := &DatabaseInfo{
		sourceDB:   os.Getenv("CARTS_SOURCEDB"),
		sourceHost: os.Getenv("CARTS_SOURCE_HOST"),
		targetHost: os.Getenv("CARTS_TARGET_HOST"),
		port:       os.Getenv("CARTS_PORT"),
	}
	db, err := getDatabase(ctx, dbInfo, dbInfo.sourceHost)
	if err != nil {
		cancel()
		t.Errorf("Error message: %s", err)
	}
	singleResult := db.RunCommand(ctx, bson.M{"listCommands": 1})

	if singleResult.Err() != nil {
		cancel()
		t.Errorf("Error message: %s", singleResult.Err())
	}
	cancel()
	fmt.Printf("Duration: %s", GetDuration())
}

// TestMongoDumpAllCollections executes mongo dump for all
// the collections in the database.
func TestMongoDumpAllCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpAllCollections()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
	}
	if err := executeMongoDump(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
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
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ONE_COLLECTION"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_2")),
	}
	if err := executeMongoDump(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
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
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_3")),
	}
	if err := executeMongoDump(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
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
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
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
		targetDB:    os.Getenv("CARTS_TARGETDB"),
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ONE_COLLECTION"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_2")),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
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
		targetDB:    os.Getenv("CARTS_TARGETDB"),
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_3")),
		args:        []string{},
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestDatabaseSync executes a synchronization of two databases
// (dump and restore operation).
func TestDatabaseSync(t *testing.T) {
	fmt.Println("\n>> TestDatabaseSync()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		targetDB:    os.Getenv("CARTS_TARGETDB"),
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestLargeDatabase executes a synchronization of two large databases.
func TestLargeDatabase(t *testing.T) {
	fmt.Println("\n>> TestLargeDatabase()")
	StartTimer()

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("TRADES_SOURCEDB"),
		targetDB:    os.Getenv("TRADES_TARGETDB"),
		sourceHost:  os.Getenv("TRADES_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("TRADES_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_TRADES_DB"),
		collections: getCollections(""),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		t.Errorf("Error message: %s", err)
	}
	fmt.Printf("Duration: %s", GetDuration())
}

// TestNotExistingSourceDB executes mongo dump on a not existing database
// and checks the expected error.
func TestNotExistingSourceDB(t *testing.T) {
	fmt.Println("\n>> TestNotExistingSourceDB()")

	dbInfo := &DatabaseInfo{
		sourceDB:    "db1",
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
	}
	err := executeMongoDump(dbInfo)
	assertError(t, errorDumpedFiles, err)
}

// TestNotAllCollectionsDumped1 executes a mongo dump of all collections,
// deletes a dumped file and checks the expected error.
func TestNotAllCollectionsDumped1(t *testing.T) {
	fmt.Println("\n>> TestNotAllCollectionsDumped1()")

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		targetDB:    os.Getenv("CARTS_TARGETDB"),
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS")),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		t.Errorf("error message: %s", err)
	}
	os.Remove(dbInfo.dumpDir + "\\" + dbInfo.sourceDB + "\\items.metadata.json")
	err := executeMongoRestore(dbInfo)
	assertError(t, errorNotAllCollsDumped, err)
}

// TestNotAllCollectionsDumped2 executes a mongo dump of a specific collection,
// deletes a dumped file and checks the expected error.
func TestNotAllCollectionsDumped2(t *testing.T) {
	fmt.Println("\n>> TestNotAllCollectionsDumped2()")

	dbInfo := &DatabaseInfo{
		sourceDB:    os.Getenv("CARTS_SOURCEDB"),
		targetDB:    os.Getenv("CARTS_TARGETDB"),
		sourceHost:  os.Getenv("CARTS_SOURCE_HOST"),
		targetHost:  os.Getenv("CARTS_TARGET_HOST"),
		port:        os.Getenv("CARTS_PORT"),
		dumpDir:     os.Getenv("DUMP_DIR_ALL_COLLECTIONS"),
		collections: getCollections(os.Getenv("CARTS_COLLECTIONS_2")),
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		t.Errorf("error message: %s", err)
	}
	os.Remove(dbInfo.dumpDir + "\\" + dbInfo.sourceDB + "\\items.metadata.json")
	err := executeMongoRestore(dbInfo)
	expected := fmt.Sprintf(errorCollectionNotFound, os.Getenv("CARTS_COLLECTIONS_2"))
	assertError(t, expected, err)
}
