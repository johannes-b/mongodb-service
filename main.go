package main

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/client"
	cloudeventshttp "github.com/cloudevents/sdk-go/pkg/cloudevents/transport/http"
	configutils "github.com/keptn/go-utils/pkg/configuration-service/utils"
	keptnevents "github.com/keptn/go-utils/pkg/events"
	keptnutils "github.com/keptn/go-utils/pkg/utils"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/mgo.v2/bson"

	mr "github.com/mongodb/mongo-tools/mongorestore"
)

type envConfig struct {
	// Port on which to listen for cloudevents
	Port int    `envconfig:"RCV_PORT" default:"8080"`
	Path string `envconfig:"RCV_PATH" default:"/"`
}

var (
	defaultHost = "localhost"
	defaultPort = "27017"
	timeout     = 10 * time.Second
	timer       = time.Now()

	// environment variables defined in ./deploy/service.yaml
	dumpDirOneCollection       = os.Getenv("DUMP_DIR_ONE_COLLECTION")
	dumpDirMultipleCollections = os.Getenv("DUMP_DIR_MULTIPLE_COLLECTIONS")
	dumpDirAllCollections      = os.Getenv("DUMP_DIR_ALL_COLLECTIONS")
)

const (
	errorNotAllCollsDumped  = "not all collections were dumped/restored"
	errorDumpedFiles        = "unable to get dumped files"
	errorCollectionNotFound = "could not find collection %s in dump directory"

	configservice = "CONFIGURATION_SERVICE"
)

// DatabaseInfo groups information from a database.
type DatabaseInfo struct {
	sourceDB    string
	targetDB    string
	sourceHost  string
	targetHost  string
	sourcePort  string
	targetPort  string
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

	stdLogger := keptnutils.NewLogger(shkeptncontext, event.Context.GetID(), "mongodb-service")
	stdLogger.Debug("Database synchronization started...")

	e := &keptnevents.ConfigurationChangeEventData{}
	if err := event.DataAs(e); err != nil {
		stdLogger.Error(fmt.Sprintf("Got Data Error: %s", err.Error()))
	}

	service := strings.ToUpper(e.Service) // in our demo example, this will be carts --> toUpper: CARTS

	var namespace string
	if e.Stage == "" {
		stage, _ := getFirstStage(e.Project)
		namespace = e.Project + "-" + stage
	} else {
		namespace = e.Project + "-" + e.Stage
	}
	stdLogger.Debug(fmt.Sprintf("namespace: %s", namespace))

	sourceDB := os.Getenv(service + "_SOURCEDB")
	if sourceDB == "" {
		stdLogger.Error(fmt.Sprintf("Invalid source database configured for %s", service))
		return
	}
	targetDB := os.Getenv(service + "_TARGETDB")
	if targetDB == "" {
		stdLogger.Error(fmt.Sprintf("Invalid target database configured for %s", service))
		return
	}
	sourceHost := os.Getenv(service + "_SOURCE_HOST")
	if sourceHost == "" {
		stdLogger.Error(fmt.Sprintf("Invalid source host configured for %s", service))
		return
	}
	targetHost := os.Getenv(service + "_TARGET_HOST")
	if targetHost == "" {
		stdLogger.Error(fmt.Sprintf("Invalid target host configured for %s", service))
		return
	}
	sourcePort := os.Getenv(service + "_SOURCE_PORT")
	if sourcePort == "" {
		stdLogger.Error(fmt.Sprintf("Invalid source port \"%s\" configured for %s", defaultPort, service))
		return
	}
	targetPort := os.Getenv(service + "_TARGET_PORT")
	if targetPort == "" {
		stdLogger.Error(fmt.Sprintf("Invalid target port \"%s\" configured for %s", defaultPort, service))
		return
	}

	dbInfo := &DatabaseInfo{
		sourceDB:    sourceDB,
		targetDB:    targetDB,
		sourceHost:  sourceHost + "." + namespace,
		targetHost:  targetHost + "." + namespace,
		sourcePort:  sourcePort,
		targetPort:  targetPort,
		dumpDir:     os.Getenv("DUMP_DIR"),
		collections: getCollections(os.Getenv(service + "_COLLECTIONS")),
		args: []string{
			mr.DropOption,
		},
	}

	StartTimer()

	stdLogger.Debug(fmt.Sprintf("Starting to execute mongo dump on database %s on host %s...", dbInfo.sourceDB, dbInfo.sourceHost))
	if err := executeMongoDump(dbInfo); err != nil {
		stdLogger.Error(fmt.Sprintf("Failed to execute mongo dump on database %s: %s", dbInfo.sourceDB, err.Error()))
	} else {
		stdLogger.Debug("Concluded mongo dump successfully!")
		stdLogger.Debug(fmt.Sprintf("Starting to execute mongo restore on database %s on host %s...", dbInfo.targetDB, dbInfo.targetHost))
		if err := executeMongoRestore(dbInfo); err != nil {
			stdLogger.Error(fmt.Sprintf("Failed to execute mongo restore on database  %s: %s", dbInfo.targetDB, err.Error()))
		} else {
			stdLogger.Debug("Concluded mongo restore successfully!")
			stdLogger.Debug(fmt.Sprintf("Duration of snapshot synchronization: %s", GetDuration()))
		}
	}
	stdLogger.Debug("Deleting dump directory...")
	os.RemoveAll(dbInfo.dumpDir + "/")
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
func getDatabase(ctx context.Context, dbInfo *DatabaseInfo, host string) (*mongo.Database, error) {
	var hostURL string
	var db string
	var port string
	if host == "source" {
		hostURL = dbInfo.sourceHost
		db = dbInfo.sourceDB
		port = dbInfo.sourcePort
	} else {
		hostURL = dbInfo.targetHost
		db = dbInfo.targetDB
		port = dbInfo.targetPort
	}
	uri := "mongodb://" + hostURL + ":" + port //mongodb://carts-db:27017
	fmt.Printf("Uri: %s\n", uri)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return client.Database(db), nil
}

// getCollections converts a string with collection names to a string array.
func getCollections(collections string) []string {
	if collections == "" {
		return []string{}
	}
	parts := strings.Split(collections, ";")
	colArr := make([]string, len(parts))
	for i, part := range parts {
		colArr[i] = part
	}
	return colArr
}

// assertDatabaseConsistency checks if all collections in the directory are
// available also in the database.
func assertDatabaseConsistency(dbInfo *DatabaseInfo, host string) error {
	collectionNamesDB, err := getCollectionNames(dbInfo, host)
	if err != nil {
		return err
	}

	files, err := getDumpedFiles(dbInfo)
	if err != nil {
		return fmt.Errorf(errorDumpedFiles)
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
			return fmt.Errorf(errorNotAllCollsDumped)
		}
	} else {
		for _, col := range dbInfo.collections {
			if !contains(collectionNamesDump, col) {
				return fmt.Errorf(errorCollectionNotFound, col)
			}
		}
	}
	fmt.Println("Concluded file check successfully")
	return nil
}

//getCollectionNames returns the collection names from a database.
func getCollectionNames(dbInfo *DatabaseInfo, host string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	db, err := getDatabase(ctx, dbInfo, host)
	if err != nil {
		cancel()
		return nil, err
	}
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	cancel()
	return collections, err
}

// getFiles returns the files from a dump directory.
func getDumpedFiles(dbInfo *DatabaseInfo) ([]os.FileInfo, error) {
	dumpdir := dbInfo.dumpDir + "/" + dbInfo.sourceDB
	fmt.Printf("Dumpdir: %s\n", dumpdir)
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

// StartTimer sets the current time for time measurement
func StartTimer() {
	timer = time.Now()
}

// GetDuration returns the time passed since the timer started
func GetDuration() time.Duration {
	return time.Since(timer)
}

func getFirstStage(project string) (string, error) {
	url, err := url.Parse(os.Getenv(configservice))
	if err != nil {
		return "", fmt.Errorf("Failed to retrieve value from ENVIRONMENT_VARIABLE: %s", configservice)
	}

	if url.Scheme == "" {
		url.Scheme = "http"
	}

	resourceHandler := configutils.NewResourceHandler(url.String())
	handler := keptnutils.NewKeptnHandler(resourceHandler)

	shipyard, err := handler.GetShipyard(project)
	if err != nil {
		return "", err
	}

	return shipyard.Stages[0].Name, nil
}
