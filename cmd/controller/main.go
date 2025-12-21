package main

import (
	"context"
	"fmt"
	"path/filepath"
	// "os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	// "github.com/MagikMowgli/k8s-operator/pkg/bigquery"

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

func reconcile(event watch.Event) error {
	table, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unexpected object type: %T", event.Object)
}
	resourceName := table.GetName()
	namespace := table.GetNamespace()

	fmt.Printf("Reconciling %s %s (event=%s)\n", namespace, resourceName, event.Type)
	return nil
}
