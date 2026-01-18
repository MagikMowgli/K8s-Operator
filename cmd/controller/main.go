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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"github.com/MagikMowgli/k8s-operator/pkg/bigquery"
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

	gvr := schema.GroupVersionResource{
		Group:    "mahdi.dev",
		Version:  "v1",
		Resource: "bigquerytables",
	}

	fmt.Println("\nNow watching for BigQueryTable changes...")

	sendInitialEvents := false
	watcher, err := dynClient.Resource(gvr).Namespace("").Watch(context.Background(), metav1.ListOptions{
		SendInitialEvents:      &sendInitialEvents,
		ResourceVersionMatch:   metav1.ResourceVersionMatchNotOlderThan,
	})
	if err != nil {
		panic(err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		if err := reconcile(context.Background(), dynClient, gvr, event); err != nil {
			fmt.Printf("Error reconciling event: %v\n", err)
		}
	}
}

func reconcile(
	ctx context.Context,
	dynClient dynamic.Interface,
	gvr schema.GroupVersionResource,
	event watch.Event,
) error {

	// 1) Get name/namespace from the event object
	obj, ok := event.Object.(*unstructured.Unstructured)
	if !ok {
		return fmt.Errorf("unexpected object type: %T", event.Object)
	}
	name := obj.GetName()
	namespace := obj.GetNamespace()

	fmt.Printf("Reconciling %s/%s (event=%s)\n", namespace, name, event.Type)

	// 2) GET the current object from the API server (authoritative desired state)
	current, err := dynClient.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			// CR doesn't exist anymore (cannot read spec here unless using finalizers)
			fmt.Printf("CR not found in Kubernetes anymore (already gone)\n")
			return nil
		}
		return err
	}

	// 3) Determine if Kubernetes is deleting this object
	deleting := current.GetDeletionTimestamp() != nil
	fmt.Printf("deleting=%v\n", deleting)

	// 4) If NOT deleting, ensure finalizer exists (so we can clean up later)
	if !deleting && !hasFinalizer(current, finalizerName) {
		fmt.Printf("Adding finalizer to %s/%s\n", namespace, name)
		addFinalizer(current, finalizerName)

		_, err := dynClient.Resource(gvr).Namespace(namespace).Update(ctx, current, metav1.UpdateOptions{})
		return err
	}

	// 5) Read spec (we can do this for both create + delete paths because current still exists)
	project, dataset, tableName, err := readSpecWithDefaults(current, name)
	if err != nil {
		return err
	}

	// 6) If deleting: clean up BigQuery and then remove finalizer so Kubernetes can finish delete
	if deleting {
		if hasFinalizer(current, finalizerName) {
			fmt.Printf("Deleting BigQuery table %s.%s (project=%s)\n", dataset, tableName, project)

			if err := bigquery.DeleteTable(project, dataset, tableName); err != nil {
				return fmt.Errorf("failed to delete BigQuery table: %v", err)
			}

			fmt.Printf("Removing finalizer %q from %s/%s\n", finalizerName, namespace, name)
			removeFinalizer(current, finalizerName)

			_, err := dynClient.Resource(gvr).Namespace(namespace).Update(ctx, current, metav1.UpdateOptions{})
			return err
		}
		return nil
	}

	// 7) If NOT deleting: ensure BigQuery table exists (desired state = CR exists)
	exists, err := bigquery.TableExists(project, dataset, tableName)
	if err != nil {
		return err
	}

	if !exists {
		fmt.Printf("BigQuery table missing -> creating %s.%s (project=%s)\n", dataset, tableName, project)
		return bigquery.CreateTable(project, dataset, tableName)
	}

	fmt.Printf("BigQuery table exists -> in sync\n")
	return nil
}

// ----- helpers -----

func readSpecWithDefaults(obj *unstructured.Unstructured, defaultTableName string) (project, dataset, tableName string, err error) {
	project, _, _ = unstructured.NestedString(obj.Object, "spec", "project")
	dataset, _, _ = unstructured.NestedString(obj.Object, "spec", "dataset")
	tableName, _, _ = unstructured.NestedString(obj.Object, "spec", "tableName")

	if tableName == "" {
		tableName = defaultTableName
	}

	if project == "" {
		project = os.Getenv("GCP_PROJECT_ID")
		if project == "" {
			return "", "", "", fmt.Errorf("GCP project not specified in spec and GCP_PROJECT_ID env var not set")
		}
	}

	if dataset == "" {
		return "", "", "", fmt.Errorf("dataset must be set in spec.dataset")
	}

	return project, dataset, tableName, nil
}

func hasFinalizer(obj *unstructured.Unstructured, f string) bool {
	for _, existing := range obj.GetFinalizers() {
		if existing == f {
			return true
		}
	}
	return false
}

func addFinalizer(obj *unstructured.Unstructured, f string) {
	obj.SetFinalizers(append(obj.GetFinalizers(), f))
}

func removeFinalizer(obj *unstructured.Unstructured, f string) {
	finalizers := obj.GetFinalizers()
	kept := make([]string, 0, len(finalizers))

	for _, existing := range finalizers {
		if existing != f {
			kept = append(kept, existing)
		}
	}
	obj.SetFinalizers(kept)
}
