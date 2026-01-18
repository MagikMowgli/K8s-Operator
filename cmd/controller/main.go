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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const finalizerName = "bigquerytables.mahdi.dev/finalizer"

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
		if err := reconcile(context.Background(), dynClient, databaseGVR, event); err != nil {
			fmt.Printf("Error reconciling event: %v\n", err)
		}
}
}

// Reconcile function to handle events from the watcher
func reconcile(
	ctx context.Context,
	dynClient dynamic.Interface,
	gvr schema.GroupVersionResource,
	event watch.Event,
) error {

	obj, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unexpected object type: %T", event.Object)
	}

	name := obj.GetName()
	namespace := obj.GetNamespace()

	fmt.Printf("Reconciling %s %s (event=%s)\n", namespace, name, event.Type)

	current, err := dynClient.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			fmt.Printf("Desired state: CR is gone -> ensure BigQuery table is deleted\n")
			return nil
		} 
		return err
	}

	deleting := current.GetDeletionTimestamp() != nil
	fmt.Printf("deleting=%v\n", deleting)
	return nil
}


func hasfinalizer(obj *unstructured.Unstructured, finalizer string) bool {
	for _, exisiting := range obj.GetFinalizers() {
		if exisiting == finalizer {
			return true
		}
	}
	return false
}

func addfinalizer(obj *unstructured.Unstructured, finalizer string) {
	obj.SetFinalizers(append(obj.GetFinalizers(), finalizer))
}

