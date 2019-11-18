# JMeter Service

The *mongodb-service* is a Keptn service used for ...

The *mongodb-service* listens to Keptn events of type:
- `sh.keptn.event.configuration.change`

What does the service??? 

## Installation



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

To delete a deployed *jmeter-service*, use the file `deploy/service.yaml` from this repository and delete the Kubernetes resources:

```console
kubectl delete -f deploy/service.yaml
```
