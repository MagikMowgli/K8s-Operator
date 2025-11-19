package main

import (
	"context"
	"fmt"
	"path/filepath"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)


func main() {
	kubeconfig := filepath.Join(homedir.HomeDir(), ".kube", "config")

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		panic(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	fmt.Println("\n Now watching for pod changes...")
	SendInitialEvents := false
	watcher, err := clientset.CoreV1().Pods("").Watch(context.TODO(), metav1.ListOptions{SendInitialEvents: &SendInitialEvents, 
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan})
	if err != nil {
		panic(err)
	}

	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		pod := event.Object.(*v1.Pod)
		switch event.Type {
		case watch.Added:
			fmt.Printf("✅ NEW POD CREATED: %s in namespace %s\n", pod.Name, pod.Namespace)
    	case watch.Deleted:
        	fmt.Printf("❌ POD DELETED: %s in namespace %s\n", pod.Name, pod.Namespace)
    	case watch.Modified:
        	fmt.Printf("🔄 POD MODIFIED: %s (status: %s)\n", pod.Name, pod.Status.Phase)
    	}
	}	
}