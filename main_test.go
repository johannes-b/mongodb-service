// For the tests a mongo database named "carts-db" was created
//containing the collections items, categories and users.
package main

import (
	"context"
	"fmt"
	"testing"

	mr "github.com/mongodb/mongo-tools/mongorestore"
	"gopkg.in/mgo.v2/bson"
)

// TestMongoDriver instantiates the mongo driver.
func TestMongoDriver(t *testing.T) {
	fmt.Println("\n>> TestMongoDriver()")

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	dbInfo := &DatabaseInfo{
		name: cartsDB,
		host: defaultHost,
		port: defaultPort,
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

	dbInfo := &DatabaseInfo{
		name:        cartsDB,
		host:        defaultHost,
		port:        defaultPort,
		dumpDir:     dumpDirAllCollections,
		collections: []string{},
	}
	err := executeMongoDump(dbInfo)
	if err != nil {
		fail(err, t)
	}
}

// TestMongoDumpOneCollection executes mongo dump for the
// categories collection.
func TestMongoDumpOneCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpOneCollection()")

	dbInfo := &DatabaseInfo{
		name:    cartsDB,
		host:    defaultHost,
		port:    defaultPort,
		dumpDir: dumpDirOneCollection,
		collections: []string{
			itemsCol,
		},
	}
	err := executeMongoDump(dbInfo)
	if err != nil {
		fail(err, t)
	}
}

// TestMongoDumpMultipleCollections executes mongo dump for the
// multiple collections.
func TestMongoDumpMultipleCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpMultipleCollections()")

	dbInfo := &DatabaseInfo{
		name:    cartsDB,
		host:    defaultHost,
		port:    defaultPort,
		dumpDir: dumpDirMultipleCollections,
		collections: []string{
			itemsCol,
			categoriesCol,
		},
	}
	err := executeMongoDump(dbInfo)
	if err != nil {
		fail(err, t)
	}
}

// TestMongoRestoreAllCollections executes mongo restore for all
// the collections in the database.
func TestMongoRestoreAllCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreAllCollections()")

	dbInfo := &DatabaseInfo{
		name:        "carts-db-test",
		host:        defaultHost,
		port:        defaultPort,
		dumpDir:     dumpDirAllCollections,
		sourceDB:    cartsDB,
		collections: []string{},
		args: []string{
			mr.DropOption,
		},
	}
	err := executeMongoRestore(dbInfo)
	if err != nil {
		fail(err, t)
	}
}

// TestMongoRestoreOneCollection executes mongo restore for
// the categories collection.
func TestMongoRestoreOneCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreOneCollection()")

	dbInfo := &DatabaseInfo{
		name:     "carts-db-test-2",
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  dumpDirAllCollections,
		sourceDB: cartsDB,
		collections: []string{
			itemsCol,
		},
		args: []string{
			mr.DropOption,
		},
	}
	err := executeMongoRestore(dbInfo)
	if err != nil {
		fail(err, t)
	}
}

// TestMongoRestoreMultipleCollection executes mongo restore for
// the categories collection.
func TestMongoRestoreMultipleCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreMultipleCollection()")

	dbInfo := &DatabaseInfo{
		name:     "carts-db-test-3",
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  dumpDirAllCollections,
		sourceDB: cartsDB,
		collections: []string{
			itemsCol,
			categoriesCol,
		},
		args: []string{},
	}
	err := executeMongoRestore(dbInfo)
	if err != nil {
		fail(err, t)
	}
}
