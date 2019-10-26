package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	cloudeventshttp "github.com/cloudevents/sdk-go/pkg/cloudevents/transport/http"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"

	commonopts "github.com/mongodb/mongo-tools-common/options"
	md "github.com/mongodb/mongo-tools/mongodump"
	mr "github.com/mongodb/mongo-tools/mongorestore"
)

type envConfig struct {
	// Port on which to listen for cloudevents
	Port int    `envconfig:"RCV_PORT" default:"8080"`
	Path string `envconfig:"RCV_PATH" default:"/"`
}

var (
	prodDb                    = "carts-db"
	testDb                    = "carts-db-test"
	testDb2                   = "carts-db-test-2"
	defaultHost               = "localhost"
	defaultPort               = "27017"
	dumpDirAllCollections     = "./dumpdir/dumpAllCollections"
	dumpDirSpecificCollection = "./dumpdir/dumpSpecificCollection"
	itemsCol                  = "items"
	categoriesCol             = "categories"
	timeout                   = 10 * time.Second
	dropDatabase              = true
)

// DatabaseInfo groups information from a database.
type DatabaseInfo struct {
	name       string
	host       string
	port       string
	dumpDir    string
	sourceDB   string
	collection string
}

func main() {
	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Fatalf("Failed to process env var: %s", err)
	}
	os.Exit(_main(os.Args[1:], env))
}

func gotEvent(ctx context.Context, event cloudevents.Event) error {
	var shkeptncontext string
	event.Context.ExtensionAs("shkeptncontext", &shkeptncontext)

	go syncTestDB(event, shkeptncontext)

	return nil
}

func syncTestDB(event cloudevents.Event, shkeptncontext string) {
	fmt.Println("here comes the business logic to dump/restore test db")
}

func _main(args []string, env envConfig) int {

	ctx := context.Background()

	t, err := cloudeventshttp.New(
		cloudeventshttp.WithPort(env.Port),
		cloudeventshttp.WithPath(env.Path),
	)

	if err != nil {
		log.Fatalf("failed to create transport, %v", err)
	}
	c, err := client.New(t)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Fatalf("failed to start receiver: %s", c.StartReceiver(ctx, gotEvent))

	return 0
}

// getDatabase returns a Database instance.
func getDatabase(ctx context.Context, dbInfo *DatabaseInfo) (*mongo.Database, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+dbInfo.host+":"+dbInfo.port))
	if err != nil {
		return nil, err
	}
	return client.Database(dbInfo.name), nil
}

func fail(err error, t *testing.T) {
	fmt.Println(err)
	t.Fail()
}

//------------ Mongo Dump --------------

// getMongoDump returns an initialized MongoDump object.
func getMongoDump(dbInfo *DatabaseInfo) *md.MongoDump {
	connection := &commonopts.Connection{
		Host: dbInfo.host,
		Port: dbInfo.port,
	}
	toolOptions := &commonopts.ToolOptions{
		Connection: connection,
		Namespace:  &commonopts.Namespace{DB: dbInfo.name},
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

	if dbInfo.collection != "" {
		toolOptions.Namespace.Collection = dbInfo.collection
	}

	return &md.MongoDump{
		ToolOptions:   toolOptions,
		InputOptions:  inputOptions,
		OutputOptions: outputOptions,
	}
}

// executeMongoDump processes a mongodump operation.
func executeMongoDump(dbInfo *DatabaseInfo) error {
	mongoDump := getMongoDump(dbInfo)

	err := mongoDump.Init()
	if err != nil {
		return err
	}

	err = mongoDump.Dump()
	if err != nil {
		return err
	}

	err = assertDatabaseConsistency(dbInfo)
	if err != nil {
		return err
	}
	return nil
}

//------------ Mongo Restore --------------

// getMongoRestore returns an initialized MongoRestore object.
func getMongoRestore(targetDir string, database string) (*mr.MongoRestore, error) {
	args := []string{
		//mr.DropOption,
	}
	opts, err := mr.ParseOptions(args, "", "")

	if err != nil {
		return nil, err
	}

	restore, err := mr.New(opts)
	if err != nil {
		return nil, err
	}
	restore.TargetDirectory = targetDir
	restore.NSOptions.DB = database
	return restore, nil
}

// executeMongoRestore processes a restore operation.
func executeMongoRestore(dbInfo *DatabaseInfo, targetDir string) error {
	restore, err := getMongoRestore(targetDir, dbInfo.name)
	if err != nil {
		return err
	}

	if dropDatabase {
		session, _ := restore.SessionProvider.GetSession()
		db := session.Database(dbInfo.name)

		err = db.Drop(nil)
		if err != nil {
			return err
		}
		fmt.Println("Database " + restore.NSOptions.DB + " dropped successfully!")
	}

	result := restore.Restore()
	if result.Err != nil {
		return result.Err
	}

	err = assertDatabaseConsistency(dbInfo)
	if err != nil {
		return err
	}
	return nil
}

//------------  --------------

// assertDatabaseConsistency checks if all collections in the directory are
// available also in the database.
func assertDatabaseConsistency(dbInfo *DatabaseInfo) error {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	db, err := getDatabase(ctx, dbInfo)

	if err != nil {
		return err
	}

	collectionNames, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return err
	}

	files, err := ioutil.ReadDir(dbInfo.dumpDir + "/" + dbInfo.sourceDB)
	if err != nil {
		fmt.Println("An error occured while checking the files in the dump directory!")
		return err
	}

	fmt.Println()
	numCollections := 0
	collection := ""
	for i, file := range files {
		if i%2 != 0 {
			bsonFile := files[i-1].Name()
			jsonFile := file.Name()
			containsBSON := false
			containsJSON := false

			for _, name := range collectionNames {
				if bsonFile == (name + ".bson") {
					containsBSON = true
				}
				if jsonFile == (name + ".metadata.json") {
					containsJSON = true
				}
			}
			if (!containsBSON || !containsJSON) && dbInfo.collection == "" {
				return fmt.Errorf("Could not find dumped files for " + bsonFile + " and " + jsonFile)
			}
			numCollections = numCollections + 1
			collection = bsonFile[:strings.LastIndex(bsonFile, ".bson")]
			fmt.Println("Found dumped files for collection " + collection + "!")

			if dbInfo.collection == collection {
				break
			}
		}
	}
	if dbInfo.collection != "" && dbInfo.collection != collection {
		return fmt.Errorf("specified collection was not dumped")
	}
	if dbInfo.collection == "" && numCollections != len(collectionNames) {
		return fmt.Errorf("dumped and restored collections do not correspond")
	}
	fmt.Println("Found all collections in dump directory!")
	return nil
}
