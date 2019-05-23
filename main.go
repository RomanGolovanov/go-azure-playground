package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/fatih/structs"
	"github.com/mitchellh/mapstructure"
	"github.com/Azure/azure-sdk-for-go/storage"
)

type myData struct {
	StringField string
	IntField    int32
	TimeField   time.Time
	FLoatField  float32
}

func main() {

	accountName := getEnvOrDefault("ACCOUNT_NAME", storage.StorageEmulatorAccountName)
	accountKey := getEnvOrDefault("ACCOUNT_SECRET", storage.StorageEmulatorAccountKey)
	tableName := getEnvOrDefault("TABLE_NAME", "TestTable")

	client, err := storage.NewBasicClient(accountName, accountKey)
	if err != nil {
		panic(err)
	}

	tableService := client.GetTableService()
	table := tableService.GetTableReference(tableName)
	table.Create(30, storage.NoMetadata, nil)

	addRecords(table)
	queryRecords(table)

	table.Delete(30, nil)
}

func getEnvOrDefault(key, defaultValue string) string {
	res := os.Getenv(key)
	if res == "" {
		res = defaultValue
	}
	return res
}

func addRecords(table *storage.Table) {

	tableBatch := table.NewBatch()

	for i := 0; i < 10; i++ {

		d := myData{
			StringField: "test" + strconv.Itoa(i),
			IntField:    int32(i),
			TimeField:   time.Now(),
			FLoatField:  float32(math.Pi) + float32(i),
		}

		entity := &storage.Entity{}
		entity.PartitionKey = "TestPartition"
		entity.RowKey = strconv.Itoa(i)
		entity.TimeStamp = time.Now()
		entity.Table = table
		entity.Properties = structs.Map(d)

		tableBatch.InsertOrReplaceEntityByForce(entity)
	}

	err := tableBatch.ExecuteBatch()
	if err != nil {
		panic(err)
	}
}

func queryRecords(table *storage.Table) {

	queryOptions := &storage.QueryOptions{}
	queryOptions.Filter = "(PartitionKey eq 'TestPartition')"

	res, err := table.QueryEntities(30, storage.MinimalMetadata, queryOptions)
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(res.Entities); i++ {
		entity := res.Entities[i]
		props := entity.Properties

		var d myData
		mapstructure.Decode(props, &d)

		fmt.Println(d.StringField, d.IntField, d.TimeField, d.FLoatField)
	}
}
