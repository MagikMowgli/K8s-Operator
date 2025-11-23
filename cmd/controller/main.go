package main

import (
	"context"
	"fmt"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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
		Resource: "databases",
	}

	fmt.Println("\n Now watching for Database changes...")
	SendInitialEvents := false
	watcher, err := dynClient.Resource(databaseGVR).Namespace("").Watch(context.Background(), metav1.ListOptions{SendInitialEvents: &SendInitialEvents, 
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan})
	if err != nil {
		panic(err)
	}

	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		db := event.Object.(*unstructured.Unstructured)

		name:= db.GetName()
		namespace:= db.GetNamespace()
		dbType, _, _ := unstructured.NestedString(db.Object, "spec", "type")
		dbDataset, _, _ := unstructured.NestedString(db.Object, "spec", "dataset")


		switch event.Type {
		case watch.Added:
			fmt.Printf("✅ NEW DATABASE CREATED: %s in namespace %s\n", name, namespace)
			fmt.Printf("    - Type: %s\n", dbType)
			fmt.Printf("    - Dataset: %s\n", dbDataset)
    	case watch.Deleted:
        	fmt.Printf("❌ DATABASE DELETED: %s in namespace %s\n", name, namespace)
    	case watch.Modified:
        	fmt.Printf("🔄 DATABASE MODIFIED: %s in namespace %s)\n", name, namespace)
    	}
	}	
}