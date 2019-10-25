package main

import (
	"context"
	"fmt"
	"testing"

	"gopkg.in/mgo.v2/bson"
)

// TestMongoDriver instantiates the mongo driver.
func TestMongoDriver(t *testing.T) {
	fmt.Println("\n>> TestMongoDriver()")

	ctx, _ := context.WithTimeout(context.Background(), timeout)
	dbInfo := &DatabaseInfo{
		name: prodDb,
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
	fmt.Println(singleResult.DecodeBytes())
}

// TestMongoDumpAllCollections executes mongo dump for all
// the collections in the database.
func TestMongoDumpAllCollections(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpAllCollections()")

	dbInfo := &DatabaseInfo{
		name:     prodDb,
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  dumpDirAllCollections,
		sourceDB: prodDb,
	}
	err := executeMongoDump(dbInfo)
	if err != nil {
		fail(err, t)
	}
}

// TestMongoDumpSpecificCollection executes mongo dump for the
// categories collection.
func TestMongoDumpSpecificCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoDumpSpecificCollection()")

	dbInfo := &DatabaseInfo{
		name:       prodDb,
		host:       defaultHost,
		port:       defaultPort,
		dumpDir:    dumpDirSpecificCollection,
		sourceDB:   prodDb,
		collection: categoriesCol,
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
		name:     testDb,
		host:     defaultHost,
		port:     defaultPort,
		dumpDir:  dumpDirAllCollections,
		sourceDB: prodDb,
	}
	targetDir := dbInfo.dumpDir + "/" + prodDb
	err := executeMongoRestore(dbInfo, targetDir)
	if err != nil {
		fail(err, t)
	}
}

// TestMongoRestoreSpecificCollection executes mongo restore for
// the categories collection.
func TestMongoRestoreSpecificCollection(t *testing.T) {
	fmt.Println("\n>> TestMongoRestoreSpecificCollection()")

	dbInfo := &DatabaseInfo{
		name:       testDb2,
		host:       defaultHost,
		port:       defaultPort,
		dumpDir:    dumpDirAllCollections,
		sourceDB:   prodDb,
		collection: categoriesCol,
	}
	targetDir := dbInfo.dumpDir + "/" + prodDb + "/" + dbInfo.collection + ".bson"
	err := executeMongoRestore(dbInfo, targetDir)
	if err != nil {
		fail(err, t)
	}
}
