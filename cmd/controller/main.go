package main

import (
	"context"
	"fmt"
	"path/filepath"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"github.com/MagikMowgli/k8s-operator/pkg/bigquery"

)


func main() {
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err)
	}

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	databaseGVR := schema.GroupVersionResource{
		Group:    "mahdi.dev",
		Version:  "v1",
		Resource: "bigquerytables",
	}

	fmt.Println("\n Now watching for BigQueryTable changes...")
	SendInitialEvents := false
	watcher, err := dynClient.Resource(databaseGVR).Namespace("").Watch(context.Background(), metav1.ListOptions{
		SendInitialEvents: &SendInitialEvents, 
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan})
	if err != nil {
		panic(err)
	}

	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		table := event.Object.(*unstructured.Unstructured)

		resourceName:= table.GetName()
		namespace:= table.GetNamespace()

		// Read fields from the specification
		dbType, _, _ := unstructured.NestedString(table.Object, "spec", "type")
		project, _, _ := unstructured.NestedString(table.Object, "spec", "project")  
		dataset, _, _ := unstructured.NestedString(table.Object, "spec", "dataset")
		tableName, _, _ := unstructured.NestedString(table.Object, "spec", "tableName")  


		// Default tableName to resourceName if not provided
		if tableName == "" {
			tableName = resourceName
		}

		if project == "" {
			project = os.Getenv("GCP_PROJECT_ID")

			if project == "" {
				fmt.Printf("    - Error: GCP project not specified in spec and GCP_PROJECT_ID env variable is not set.\n")
				continue
			}
		}

		switch event.Type {
		case watch.Added:
			fmt.Printf("✅ NEW BIGQUERY TABLE REQUESTED: %s in namespace %s\n", resourceName, namespace)
			fmt.Printf("    - Type: %s\n", dbType)
			fmt.Printf("    - Project: %s\n", project)
			fmt.Printf("    - Dataset: %s\n", dataset)
			fmt.Printf("    - Table Name: %s\n", tableName)

			// Create BigQuery table
			err := bigquery.CreateTable(project, dataset, tableName)
			if err != nil {
				fmt.Printf("    - Error creating BigQuery table: %v\n", err)
			} else {
				fmt.Printf("    - BigQuery table created successfully.\n")
			}
    	case watch.Deleted:
        	fmt.Printf("❌ BIGQUERY TABLE DELETED: %s in namespace %s\n", resourceName, namespace)
    	case watch.Modified:
        	fmt.Printf("🔄 BIGQUERY TABLE MODIFIED: %s in namespace %s)\n", resourceName, namespace)
    	}
	}
}