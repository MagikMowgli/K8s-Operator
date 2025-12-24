package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

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
		if err := reconcile(event); err != nil {
			fmt.Printf("Error reconciling event: %v\n", err)
		}
}
}

// Reconcile function to handle events from the watcher
func reconcile(event watch.Event) error {
	desiredExists := event.Type != watch.Deleted




	table, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unexpected object type: %T", event.Object)
	}

	resourceName := table.GetName()
	namespace := table.GetNamespace()

	fmt.Printf("Reconciling %s %s (event=%s)\n", namespace, resourceName, event.Type)

	dbType, _, _ := unstructured.NestedString(table.Object, "spec", "type")
    project, _, _ := unstructured.NestedString(table.Object, "spec", "project")
    dataset, _, _ := unstructured.NestedString(table.Object, "spec", "dataset")
    tableName, _, _ := unstructured.NestedString(table.Object, "spec", "tableName")

	if tableName == "" {
		tableName = resourceName
    }

    if project == "" {
        project = os.Getenv("GCP_PROJECT_ID")
        if project == "" {
            return fmt.Errorf("GCP project not specified in spec and GCP_PROJECT_ID env var not set")
        }
    }

	    switch event.Type {
    case watch.Added:
        fmt.Printf("Creating table %s.%s (type=%s)\n", dataset, tableName, dbType)
        return bigquery.CreateTable(project, dataset, tableName)

    case watch.Deleted:
        fmt.Printf("Deleting table %s.%s\n", dataset, tableName)
        return bigquery.DeleteTable(project, dataset, tableName)

    case watch.Modified:
        fmt.Printf("Modified event for %s/%s (not implemented yet)\n", namespace, resourceName)
        return nil

    default:
        return nil
    }
}
