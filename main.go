package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	cloudeventshttp "github.com/cloudevents/sdk-go/pkg/cloudevents/transport/http"
	keptnevents "github.com/keptn/go-utils/pkg/events"

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
	cartsDB       = "carts-db"
	defaultHost   = "localhost"
	defaultPort   = "27017"
	itemsCol      = "items"
	categoriesCol = "categories"
	timeout       = 10 * time.Second

	// environment variables defined in ./deploy/service.yaml
	dumpDirOneCollection       = os.Getenv("DUMP_DIR_ONE_COLLECTION")
	dumpDirMultipleCollections = os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS")
	dumpDirAllCollections      = os.Getenv("DUMP_DIR_ALL_COLLECTIONS")
)

// DatabaseInfo groups information from a database.
type DatabaseInfo struct {
	sourceDB    string
	targetDB    string
	name        string
	host        string
	port        string
	dumpDir     string
	collections []string
	args        []string
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

	if event.Type() != keptnevents.ConfigurationChangeEventType {
		const errorMsg = "Received unexpected keptn event"
		return errors.New(errorMsg)
	}

	go syncTestDB(event, shkeptncontext)

	return nil
}

func syncTestDB(event cloudevents.Event, shkeptncontext string) {

	dbInfo := &DatabaseInfo{
		sourceDB:    cartsDB,
		targetDB:    "carts-db-test-4",
		host:        defaultHost,
		port:        defaultPort,
		dumpDir:     dumpDirAllCollections,
		collections: []string{},
		args: []string{
			mr.DropOption,
		},
	}
	if err := executeMongoDump(dbInfo); err != nil {
		//TODO
	}
	if err := executeMongoRestore(dbInfo); err != nil {
		//TODO
	}
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
	return client.Database(dbInfo.sourceDB), nil
}

func fail(err error, t *testing.T) {
	fmt.Println(err)
	t.Fail()
}

// getMongoDump returns an initialized MongoDump object.
func getMongoDump(dbInfo *DatabaseInfo) *md.MongoDump {
	connection := &commonopts.Connection{
		Host: dbInfo.host,
		Port: dbInfo.port,
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

// executeMongoDump processes a mongodump operation.
func executeMongoDump(dbInfo *DatabaseInfo) error {
	if len(dbInfo.collections) == 0 { //dump all collections
		fmt.Println("Dumping all collections from " + dbInfo.sourceDB)
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
	if err := assertDatabaseConsistency(dbInfo); err != nil {
		return err
	}
	return nil
}

// initAndDump initializes a MongoDump Object and restores collections.
func initAndDump(dbInfo *DatabaseInfo, col string) error {
	mongoDump := getMongoDump(dbInfo)
	mongoDump.ToolOptions.Collection = col

	if err := mongoDump.Init(); err != nil {
		return err
	}
	if err := mongoDump.Dump(); err != nil {
		return err
	}
	return nil
}

// getMongoRestore returns an initialized MongoRestore object.
func getMongoRestore(database string, targetDir string, args []string) (*mr.MongoRestore, error) {
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
func executeMongoRestore(dbInfo *DatabaseInfo) error {

	if len(dbInfo.collections) == 0 {
		targetDir := dbInfo.dumpDir + "/" + dbInfo.sourceDB
		if err := initAndRestore(dbInfo.targetDB, targetDir, dbInfo.args); err != nil {
			return err
		}
	} else {
		for _, col := range dbInfo.collections {
			targetDir := dbInfo.dumpDir + "/" + dbInfo.sourceDB + "/" + col + ".bson"
			if err := initAndRestore(dbInfo.targetDB, targetDir, dbInfo.args); err != nil {
				return err
			}
		}
	}
	if err := assertDatabaseConsistency(dbInfo); err != nil {
		return err
	}
	return nil
}

// initAndRestore initializes a MongoRestore Object and restores collections.
func initAndRestore(dbname string, targetDir string, args []string) error {
	restore, err := getMongoRestore(dbname, targetDir, args)
	if err != nil {
		return err
	}
	if result := restore.Restore(); result.Err != nil {
		return result.Err
	}
	return nil
}

//------------  --------------

// assertDatabaseConsistency checks if all collections in the directory are
// available also in the database.
func assertDatabaseConsistency(dbInfo *DatabaseInfo) error {
	collectionNamesDB, err := getCollectionNames(dbInfo)
	if err != nil {
		return err
	}

	files, err := getDumpedFiles(dbInfo)
	if err != nil {
		fmt.Println("An error occured while checking the files in the dump directory!")
		return err
	}
	collectionNamesDump := make([]string, len(files)/2)

	for i, file := range files {
		if i%2 != 0 {
			bsonFile := files[i-1].Name()
			jsonFile := file.Name()
			containsBSON := false
			containsJSON := false

			for _, name := range collectionNamesDB {
				if bsonFile == (name + ".bson") {
					containsBSON = true
				}
				if jsonFile == (name + ".metadata.json") {
					containsJSON = true
				}
			}

			if containsBSON && containsJSON {
				collection := bsonFile[:strings.LastIndex(bsonFile, ".bson")]
				collectionNamesDump[i/2] = collection
			}
		}
	}

	sort.Strings(collectionNamesDump)
	sort.Strings(collectionNamesDB)

	if len(dbInfo.collections) == 0 {
		if !reflect.DeepEqual(collectionNamesDump, collectionNamesDB) {
			return fmt.Errorf("not all collections were dumped/restored")
		}
	} else {
		for _, col := range dbInfo.collections {
			if !contains(collectionNamesDump, col) {
				return fmt.Errorf("could not find collection " + col + "in dump directory")
			}
		}
	}
	return nil
}

//getCollectionNames returns the collection names from a database.
func getCollectionNames(dbInfo *DatabaseInfo) ([]string, error) {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	db, err := getDatabase(ctx, dbInfo)

	if err != nil {
		return nil, err
	}
	return db.ListCollectionNames(ctx, bson.M{})
}

// getFiles returns the files from a dump directory.
func getDumpedFiles(dbInfo *DatabaseInfo) ([]os.FileInfo, error) {
	dumpdir := dbInfo.dumpDir + "/" + dbInfo.name
	if dbInfo.sourceDB != "" {
		dumpdir = dbInfo.dumpDir + "/" + dbInfo.sourceDB
	}
	return ioutil.ReadDir(dumpdir)
}

//contains checks if the array contains a string.
func contains(arr []string, s string) bool {
	for _, n := range arr {
		if s == n {
			return true
		}
	}
	return false
}
