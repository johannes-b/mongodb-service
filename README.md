# Mongodb-Service

The *mongodb-service* is a Keptn service used for synchronizing two mongo databases. A usecase is for instance a synchronization of a canary or test database with the data of a production database. 

The *mongodb-service* listens to Keptn events of type:
- `sh.keptn.event.configuration.change`

In the synchronization process the service executes a mongo dump on the production database and stores the dumped files in the PVC. After dumping, the service performs a check of the dumped files to validate if the process was successful. To import the data in the canary database, the service performes a mongo restore operation and validates this process.  

This service allows to synchronize the entire database or only specific collections and to perform the synchronization on databases that are located on two different hosts. 

## Installation

//TODO 

Modify the parameters in the `configmap.yaml`file according to your requirements. The parameter name until the underscore should match to the name of your service. To synchronize only specific collections, use a semicolon seperated list of strings, for instance `"col1;col2;col3"`.  

## Deploy in your Kubernetes cluster

1. PVC

2. ConfigMap

3. Service and Deplyoment

    To deploy the current version of the *mongodb-service* in your Keptn Kubernetes cluster, use the file `deploy/service.yaml` from this repository and apply it:

    ```console
    kubectl apply -f deploy/service.yaml
    ```

4. Activate event listener

    ```console
    kubectl apply -f deploy/distributor.yaml
    ```

## Delete in your Kubernetes cluster

To delete a deployed *mongodb-service*, use the file `deploy/service.yaml` from this repository and delete the Kubernetes resources:

```console
kubectl delete -f deploy/service.yaml
```
