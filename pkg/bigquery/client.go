package bigquery

import (
	"context"
	"cloud.google.com/go/bigquery"
)


func CreateTable(projectID, datasetID, tableID string) error {
	ctx := context.Background()
	metadata := &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.StringFieldType},
			{Name: "created_at", Type: bigquery.TimestampFieldType},
		},
	}

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	defer client.Close()

	dataset := client.Dataset(datasetID)
	table := dataset.Table(tableID)

	err = table.Create(ctx, metadata)
	if err != nil {
		return err
	}

	return nil
}