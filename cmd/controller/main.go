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

	// If Kubernetes is deleting this CR, we must cleanup BigQuery BEFORE letting k8s remove the CR
	if deleting {
		// Only attempt to delete the BigQuery table if the finalizer is present
		if hasfinalizer(current, finalizerName) {
			// Read spec from "current" (it still exists because finalizer is blocking deletion)
			project, _, _ := unstructured.NestedString(current.Object, "spec", "project")
			dataset, _, _ := unstructured.NestedString(current.Object, "spec", "dataset")
			tableName, _, _ := unstructured.NestedString(current.Object, "spec", "tableName")

			// Default tableName to CR name if not specified
			if tableName == "" {
				tableName = name
			}

			// Fallback project to env var if spec.project is missing
			if project == "" {
				project = os.Getenv("GCP_PROJECT_ID")
				if project == "" {
					return fmt.Errorf("GCP project not specified in spec and GCP_PROJECT_ID env var not set")
				}
			}

			// 1) Cleanup BigQuery table (external resource)
			if err := bigquery.DeleteTable(project, dataset, tableName); err != nil {
				return fmt.Errorf("failed to delete BigQuery table: %v", err)
			}

			// 2) Remove finalizer to allow k8s to delete the CR
			removefinalizer(current, finalizerName)

			// Persist the finalizer removal to the API server
			if _, err := dynClient.Resource(gvr).Namespace(namespace).Update(ctx, current, metav1.UpdateOptions{}); err != nil {
				return fmt.Errorf("failed to remove finalizer: %v", err)
			}
		} 

		return nil
		}

	// If the CR is is NOT being deleted, make sure the finalizer is present
	if !deleting && !hasfinalizer(current, finalizerName) {
		fmt.Printf("Adding finalizer to %s/%s\n", namespace, name)

		addfinalizer(current, finalizerName)

		_, err = dynClient.Resource(gvr).Namespace(namespace).Update(ctx, current, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		return nil
	}
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

func removefinalizer(obj *unstructured.Unstructured, finalizer string) {
	finalizers := obj.GetFinalizers()

	kept := make([]string, 0, len(finalizers))
	for _, existing := range finalizers {
		if existing != finalizer {
			// if this finalizer is NOT the one to remove, keep it
			kept = append(kept, existing)
		}
	}
	obj.SetFinalizers(kept)
}
