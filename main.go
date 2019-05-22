package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/storage"
)

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

		m := make(map[string]interface{})
		m["StringField"] = "foo" + strconv.Itoa(i)
		m["IntField"] = int32(i)
		m["TimeField"] = time.Now()
		m["FLoatField"] = float32(math.Pi)

		entity := &storage.Entity{}
		entity.PartitionKey = "TestPartition"
		entity.RowKey = strconv.Itoa(i)
		entity.TimeStamp = time.Now()
		entity.Table = table
		entity.Properties = m

		tableBatch.InsertOrReplaceEntityByForce(entity)
	}

	tableBatch.ExecuteBatch()
}

func queryRecords(table *storage.Table) {

	queryOptions := &storage.QueryOptions{}
	queryOptions.Filter = "(PartitionKey eq 'TestPartition')"

	res, err := table.QueryEntities(30, storage.FullMetadata, queryOptions)
	if err != nil {
		panic(err)
	}

	for i := 0; i < len(res.Entities); i++ {
		entity := res.Entities[i]
		props := entity.Properties
		fmt.Println(
			entity.PartitionKey,
			entity.RowKey,
			entity.TimeStamp,
			props["StringField"].(string),
			int32(props["IntField"].(float64)),
			props["TimeField"].(time.Time),
			float32(props["FLoatField"].(float64)))
	}
}
