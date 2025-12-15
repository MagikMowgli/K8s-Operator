package bigquery

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
)


func CreateTable(projectID, datasetID, tableID string) error {
	// Creates a context - A context being something that carries cancellation signals and deadlines. 
	// .Background is a root context meaning theres no timeout or no cancel its basically a default setting nothings been specified. 
	// BQ needs a context so that operations can be cancelled if needed.
	ctx := context.Background()

	// Here is where we're creating the metadata for the table structure. 
	// So this is where the columns were made which we will later use. 
	metadata := &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.StringFieldType},
			{Name: "created_at", Type: bigquery.TimestampFieldType},
		},
	}

	client, err := setupClient(ctx, projectID)
	if err != nil {
		return err
	}
	defer client.Close()

	dataset := client.Dataset(datasetID)
	table := dataset.Table(tableID)

	// Creates a table using bigquery go sdk which wraps around the bq api
	err = table.Create(ctx, metadata)
	if err != nil {
		return err
	}
	return nil
}

// Deletes a BigQuery table identified by projectID, datasetID, and tableID
func DeleteTable(projectID, datasetID, tableID string) error {
	ctx := context.Background()

	client, err := setupClient(ctx, projectID)
	if err != nil {
		return err
	}

	defer client.Close()

	dataset := client.Dataset(datasetID)
	table := dataset.Table(tableID)

	err = table.Delete(ctx)
	if err != nil && isNotFoundError(err) {
		return err
	}
	return nil 
}

// This creates a BQ client that authenticates using credentials from our environemnt (ADC - applciation default credentials)
// Communicates directly with the google bq http api 
// so essentially setting up a golang phone which will later call the bq rest api 
func setupClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Helper function to check if an error is a "not found" error from the Google API
func isNotFoundError(err error) bool {
	var gErr *googleapi.Error
	if errors.As(err, &gErr) {
		return gErr.Code == 404
	}
	return false
}