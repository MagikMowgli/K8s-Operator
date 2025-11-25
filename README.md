# BigQuery Kubernetes Operator

A Kubernetes operator that manages BigQuery tables declaratively.

## What It Does

- Watch for `BigQueryTable` custom resources in Kubernetes
- Automatically creates tables in Google BigQuery
- Manages table lifecycle (create, delete)

## Prerequisites

- Kubernetes cluster (minikube for local development)
- Go 1.23+
- Google Cloud project with BigQuery API enabled
- `kubectl` configured

## Setup

### 1. Install CRD
```bash
kubectl apply -f k8s/database-crd.yaml
```

### 2. Set environment variable
```bash
export GCP_PROJECT_ID=your-gcp-project-id
```

### 3. Run the operator
```bash
go run cmd/controller/main.go
```

### 4. Create a BigQuery table
```bash
kubectl apply -f k8s/example-database.yaml
```

## Project Structure
```
.
├── cmd/controller/main.go          # Main operator logic
├── pkg/bigquery/client.go          # BigQuery client wrapper
├── k8s/
│   ├── database-crd.yaml           # CustomResourceDefinition
│   └── example-database.yaml       # Example BigQueryTable resource
└── README.md
```

## Example Usage
```yaml
apiVersion: mahdi.dev/v1
kind: BigQueryTable
metadata:
  name: users-table
spec:
  type: bigquery
  dataset: my_dataset
  tableName: users
```

## What I Learned

- Building Kubernetes operators with Go
- Working with CustomResourceDefinitions (CRDs)
- Integrating with Google Cloud BigQuery API
- Kubernetes client-go library
- Environment variable configuration
- Error handling in Go

## Future Improvements

- [ ] Delete tables when BigQueryTable resource is deleted
- [ ] Update table schema when resource is modified
- [ ] Add status conditions to custom resource
- [ ] Support more BigQuery table options
- [ ] Add unit tests
- [ ] Containerize and deploy to Kubernetes

## License

MIT