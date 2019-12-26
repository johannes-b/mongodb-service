# Mongodb-Service

The *mongodb-service* is a Keptn service used for synchronizing two mongo databases. A use case is for instance a synchronization of a canary or test database with the data of a production database. 

The *mongodb-service* listens to Keptn events of type:
- `sh.keptn.event.configuration.change`

In the synchronization process the service executes a mongo dump on the source (production) database and stores the dumped files in the PVC. After dumping, the service performs a check of the dumped files to validate if the process was successful. To import the data in the target (canary) database, the service performs a mongo restore operation and validates the process.  

This service allows the synchronization of the entire database or only specific collections. Additionally, it can perform the synchronization on databases that are located on different hosts. 

## Installation

Modify the parameters in the `configmap.yaml` file and fill in your data. The parameter name until the underscore must match to the name of your service. 

Required parameters: 
* Source-database
* Source-host
* Source-port
* Target-database
* Target-host
* Target-port

Optional parameters:
* Collections: a list of collections you want to synchronize, seperated by a semicolon (`"col1;col2;col3"`). If this parameter is not set, all collections will be synchronized.  

## Deploy in your Kubernetes cluster

1. PVC

     The *mongodb-service* requires a PersistentVolumeClaim, therefore use the file `pvc.yaml` in the deploy folder from this repository and apply it:

     ```console
     kubectl apply -f deploy/pvc.yaml
     ```

2. ConfigMap

    To provide the configured environment variables for the *mongodb-service*, use the file `configmap.yaml` in the deploy folder from this repository and apply it:
 
    ```console
    kubectl apply -f deploy/configmap.yaml
    ``` 

3. Service and Deplyoment

    To deploy the current version of the *mongodb-service* in your Keptn Kubernetes cluster, use the file `service.yaml` in the deploy folder from this repository and apply it:

    ```console
    kubectl apply -f deploy/service.yaml
    ```

4. Activate event listener

    ```console
    kubectl apply -f deploy/distributor.yaml
    ```

## Delete in your Kubernetes cluster

To delete a deployed *mongodb-service*, use the file `service.yaml` in the deploy folder from this repository and delete the Kubernetes resources:

```console
kubectl delete -f deploy/service.yaml
```
