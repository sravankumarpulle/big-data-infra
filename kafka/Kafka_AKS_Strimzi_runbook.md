# Kafka on AKS with Strimzi - HA Lab Runbook

> Scope: AKS + Strimzi Operator + Kafka KRaft. This runbook matches the lab built in this session: dedicated `KafkaNodePool` objects for 3 controllers and 3 brokers, node label `workload=data-platform`, Azure Disk CSI storage, topology spreading, topic/client testing, drain/PDB troubleshooting, scaling, and upgrades.
>
> **Important:** 3 AKS worker nodes are enough to demonstrate placement and Kafka replication, but they provide no spare node capacity during a drain. For a realistic node-drain lab, use at least 4 schedulable worker nodes. Do not reduce KRaft controllers to 2 and call it HA. Keep an odd controller quorum, normally 3 for this lab.

## 1. Architecture

```text
AKS
├── Node 1: controller + broker
├── Node 2: controller + broker
├── Node 3: controller + broker
└── Optional Node 4: spare capacity for drain/failover tests

Helm
└── Strimzi Cluster Operator
    ├── Kafka CR: sre-kafka
    ├── KafkaNodePool: controllers (3)
    ├── KafkaNodePool: brokers (3)
    └── Entity Operator
        ├── Topic Operator
        └── User Operator
```

The current lab uses Kafka 4.2.0 with Strimzi 1.3.0. Before changing versions, check the Strimzi compatibility matrix and the installed CRD schema.

---

## 2. Prerequisites

Required locally or in Azure Cloud Shell:

```bash
az version
kubectl version --client
helm version
```

Required platform capabilities:

- Azure subscription and permissions to create/use AKS resources.
- AKS Kubernetes version supported by the selected Strimzi release.
- Azure Disk CSI driver enabled.
- A low-latency disk-backed StorageClass such as `managed-csi-premium` if it exists in your cluster.
- At least 3 worker nodes for the basic lab; 4 is recommended for drain/rescheduling tests.
- Sufficient CPU and memory. Check requests and allocatable capacity before expecting a displaced broker to reschedule.

Check the cluster:

```bash
kubectl get nodes -o wide
kubectl get storageclass
kubectl get csidriver
kubectl top nodes
```

If metrics are unavailable, install/enable a metrics provider before relying on `kubectl top`.

---

## 3. Optional AKS creation example

Replace placeholders for your subscription/environment. This is a lab example, not a universal production sizing recommendation.

```bash
RESOURCE_GROUP=rg-bigdata-lab
AKS_NAME=aks-bigdata-lab
LOCATION=eastus

az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION"

az aks create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_NAME" \
  --node-count 3 \
  --generate-ssh-keys

az aks get-credentials \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_NAME" \
  --overwrite-existing

kubectl get nodes -o wide
```

For a stronger HA implementation, design node pools and availability zones deliberately rather than relying on this minimal command.

---

## 4. Label the Kafka worker nodes

Get real node names first:

```bash
kubectl get nodes -o wide
```

Example names:

```bash
NODE1=aks-bigdatapool-21693461-vms1
NODE2=aks-bigdatapool-21693461-vms2
NODE3=aks-bigdatapool-21693461-vms3
```

Apply common and diagnostic labels:

```bash
kubectl label node "$NODE1" workload=data-platform environment=lab node-slot=1 --overwrite
kubectl label node "$NODE2" workload=data-platform environment=lab node-slot=2 --overwrite
kubectl label node "$NODE3" workload=data-platform environment=lab node-slot=3 --overwrite
```

Verify:

```bash
kubectl get nodes -L workload,environment,node-slot,kubernetes.io/hostname,topology.kubernetes.io/zone
```

Kafka workloads should select the common label `workload=data-platform`. Do not hard-pin broker 0/1/2 to individual `node-slot` values.

---

## 5. Namespaces

This lab keeps Strimzi and Kafka in the same namespace for simplicity:

```bash
kubectl create namespace kafka
```

For a more isolated enterprise layout, the operator can be installed in a dedicated operator namespace and configured to watch the Kafka namespace.

---

## 6. Install Strimzi with Helm

Check chart metadata:

```bash
helm show chart oci://quay.io/strimzi-helm/strimzi-kafka-operator --version 1.3.0
```

Install:

```bash
helm upgrade --install strimzi \
  oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --version 1.3.0 \
  --namespace kafka \
  --create-namespace
```

Validate:

```bash
helm list -n kafka
kubectl rollout status deployment/strimzi-cluster-operator -n kafka
kubectl get pods -n kafka
kubectl get crd | grep strimzi
```

Check the operator image:

```bash
kubectl get deployment strimzi-cluster-operator \
  -n kafka \
  -o jsonpath='{.spec.template.spec.containers[0].image}'
echo
```

Before writing CRs, query the installed CRD rather than assuming a field is supported:

```bash
kubectl explain kafkanodepool.spec
kubectl explain kafkanodepool.spec.template.pod
kubectl explain kafkanodepool.spec.template.pod.affinity
kubectl explain kafkanodepool.spec.template.pod.topologySpreadConstraints
kubectl explain kafka.spec.kafka
```

---

## 7. `kafka-controllers.yaml`

Your installed CRD rejected `spec.template.pod.nodeSelector`. Therefore this manifest uses `nodeAffinity`.

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: controllers
  namespace: kafka
  labels:
    strimzi.io/cluster: sre-kafka
spec:
  replicas: 3
  roles:
    - controller
  storage:
    type: persistent-claim
    size: 10Gi
    class: managed-csi-premium
    deleteClaim: false
  resources:
    requests:
      cpu: "250m"
      memory: "1Gi"
    limits:
      cpu: "1"
      memory: "2Gi"
  template:
    pod:
      metadata:
        labels:
          workload: kafka-controller
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: workload
                    operator: In
                    values:
                      - data-platform
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              strimzi.io/cluster: sre-kafka
              strimzi.io/pool-name: controllers
```

Validate and apply:

```bash
kubectl apply --dry-run=server -f kafka-controllers.yaml
kubectl apply -f kafka-controllers.yaml
```

---

## 8. `kafka-brokers.yaml`

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: brokers
  namespace: kafka
  labels:
    strimzi.io/cluster: sre-kafka
spec:
  replicas: 3
  roles:
    - broker
  storage:
    type: persistent-claim
    size: 50Gi
    class: managed-csi-premium
    deleteClaim: false
  resources:
    requests:
      cpu: "1"
      memory: "2Gi"
    limits:
      cpu: "2"
      memory: "4Gi"
  template:
    pod:
      metadata:
        labels:
          workload: kafka-broker
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                  - key: workload
                    operator: In
                    values:
                      - data-platform
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: kubernetes.io/hostname
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              strimzi.io/cluster: sre-kafka
              strimzi.io/pool-name: brokers
```

Validate and apply:

```bash
kubectl apply --dry-run=server -f kafka-brokers.yaml
kubectl apply -f kafka-brokers.yaml
```

---

## 9. `kafka-cluster.yaml`

The following matches the lab's Kafka 4.2.0 configuration. Revalidate the version before reuse after a Strimzi upgrade.

Before selecting a Kafka version, inspect the versions supported by your installed Strimzi release:

```sh
kubectl get configmap \
-n kafka strimzi-cluster-operator \
-o yaml
```
Save the following as kafka-cluster.yaml. Replace <SUPPORTED_KAFKA_VERSION> with a version supported by your installed operator.

```
kubectl get kafka sre-kafka -n kafka \
  -o jsonpath='{.spec.kafka.version}'
echo
````
```
4.2.0  # strimzi version output 
```

```yaml
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: sre-kafka
  namespace: kafka
spec:
  kafka:
    version: 4.2.0  #change this version 
    metadataVersion: 4.2-IV1
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
      - name: tls
        port: 9093
        type: internal
        tls: true
    config:
      default.replication.factor: 3
      min.insync.replicas: 2
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2
      num.partitions: 3
      auto.create.topics.enable: false
    rack:
      topologyKey: kubernetes.io/hostname
  entityOperator:
    topicOperator: {}
    userOperator: {}
```

Validate and apply:

```bash
kubectl apply --dry-run=server -f kafka-cluster.yaml
kubectl apply -f kafka-cluster.yaml
```

If `metadataVersion` is rejected by your installed schema/version combination, inspect:

```bash
kubectl explain kafka.spec.kafka.metadataVersion
```

and use the metadata version documented for the selected Kafka/Strimzi release rather than guessing.

---

## 10. Watch cluster creation

```bash
kubectl get kafka,kafkanodepool -n kafka
kubectl get pods -n kafka -o wide
kubectl get pvc -n kafka
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Wait for readiness:

```bash
kubectl wait kafka/sre-kafka \
  --for=condition=Ready \
  --timeout=600s \
  -n kafka
```

Display placement:

```bash
kubectl get pods -n kafka \
  -o custom-columns='POD:.metadata.name,NODE:.spec.nodeName,STATUS:.status.phase'
```

Expected design:

```text
Node 1: one controller + one broker
Node 2: one controller + one broker
Node 3: one controller + one broker
```

Actual pod numbering is controlled by Strimzi and does not need to start at the same number for controller and broker pools.

---

## 11. Storage validation

```bash
kubectl get storageclass
kubectl describe storageclass managed-csi-premium
kubectl get pvc -n kafka -o wide
kubectl get pv
```

The target is six bound claims in this lab: 3 controller claims and 3 broker claims.

If you see `WaitForFirstConsumer`, check whether a matching Kafka pod can be scheduled. It is not automatically a storage failure.

---

## 12. Create a replicated topic: `kafka-topic.yaml`

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: sre-events
  namespace: kafka
  labels:
    strimzi.io/cluster: sre-kafka
spec:
  partitions: 6
  replicas: 3
  config:
    min.insync.replicas: 2
    retention.ms: 604800000
```

Apply:

```bash
kubectl apply --dry-run=server -f kafka-topic.yaml
kubectl apply -f kafka-topic.yaml
kubectl get kafkatopic -n kafka
kubectl describe kafkatopic sre-events -n kafka
```

---

## 13. Test Kafka from an ephemeral client pod

Find the bootstrap service first:

```bash
kubectl get svc -n kafka
```

The internal bootstrap service for this cluster is normally resolved inside the namespace using the Strimzi-generated bootstrap service. Verify its exact name from the command above before use.

Start a temporary Kafka client using the same Kafka image already used by this lab:

```bash
kubectl run kafka-client \
  -n kafka \
  --restart=Never \
  --image=quay.io/strimzi/kafka:1.3.0-kafka-4.2.0 \
  --command -- sleep 3600

kubectl wait pod/kafka-client \
  -n kafka \
  --for=condition=Ready \
  --timeout=120s
```

List topics. Replace the bootstrap service if `kubectl get svc` shows a different generated name:

```bash
kubectl exec -n kafka kafka-client -- \
  bin/kafka-topics.sh \
  --bootstrap-server sre-kafka-kafka-bootstrap:9092 \
  --list
```

Describe the test topic:

```bash
kubectl exec -n kafka kafka-client -- \
  bin/kafka-topics.sh \
  --bootstrap-server sre-kafka-kafka-bootstrap:9092 \
  --describe \
  --topic sre-events
```

Producer:

```bash
kubectl exec -it -n kafka kafka-client -- \
  bin/kafka-console-producer.sh \
  --bootstrap-server sre-kafka-kafka-bootstrap:9092 \
  --topic sre-events \
  --producer-property acks=all
```

Type sample records, one per line:

```text
message-1
message-2
message-3
```

Consumer in another shell:

```bash
kubectl exec -it -n kafka kafka-client -- \
  bin/kafka-console-consumer.sh \
  --bootstrap-server sre-kafka-kafka-bootstrap:9092 \
  --topic sre-events \
  --from-beginning
```

Clean up the test pod:

```bash
kubectl delete pod kafka-client -n kafka
```

---

## 14. Broker pod-failure test

Confirm all Kafka resources are healthy first:

```bash
kubectl get kafka -n kafka
kubectl get pods -n kafka -o wide
```

Delete one broker pod:

```bash
kubectl delete pod sre-kafka-brokers-0 -n kafka
```

Watch recovery:

```bash
kubectl get pods -n kafka -w
```

Then re-run topic describe plus producer/consumer tests. Check that replicas return to a healthy ISR before performing a second disruption.

---

## 15. PodDisruptionBudget checks

List PDBs:

```bash
kubectl get pdb -n kafka
```

Inspect the Kafka PDB:

```bash
kubectl describe pdb sre-kafka-kafka -n kafka
kubectl get pdb sre-kafka-kafka -n kafka -o yaml
```

Focus on:

```text
Current Healthy
Desired Healthy
Expected Pods
Disruptions Allowed
```

If `Disruptions Allowed` is `0`, a normal `kubectl drain` can be blocked. That is protective behavior. Do not bypass it merely to make an HA demonstration succeed.

---

## 16. Node drain scenario

### 16.1 Pre-check

```bash
kubectl get kafka sre-kafka -n kafka
kubectl get pods -n kafka -o wide
kubectl get pdb -n kafka
kubectl top nodes
```

All intended Kafka pods should be healthy before the test.

### 16.2 Cordon

```bash
NODE1=aks-bigdatapool-21693461-vms1
kubectl cordon "$NODE1"
```

### 16.3 Drain

```bash
kubectl drain "$NODE1" \
  --ignore-daemonsets \
  --delete-emptydir-data \
  --force
```

### 16.4 Expected 3-node-lab limitation

With 3 brokers, 3 nodes, and strict hostname spreading using:

```yaml
whenUnsatisfiable: DoNotSchedule
```

a displaced broker can remain `Pending` because only two schedulable hostnames remain. You may see an event similar to:

```text
0/3 nodes are available:
1 node was unschedulable,
2 nodes didn't match pod topology spread constraints
```

You may also see the drain blocked by the PDB:

```text
Cannot evict pod as it would violate the pod's disruption budget
```

This is exactly why a fourth worker node with spare CPU/memory is useful for a realistic rescheduling test.

### 16.5 Recovery

Stop a repeating drain with `Ctrl+C`, then:

```bash
kubectl uncordon "$NODE1"
kubectl get nodes
kubectl get pods -n kafka -o wide -w
```

Wait for Kafka:

```bash
kubectl wait kafka/sre-kafka \
  --for=condition=Ready \
  --timeout=600s \
  -n kafka
```

Then verify PDB and topic health again.

---

## 17. Do NOT "reduce PDB to 4 then 2 controllers" as an HA fix

A PDB and a Kafka replica count solve different problems:

- **KafkaNodePool `replicas`** controls how many controllers/brokers exist.
- **PDB** controls how voluntary Kubernetes disruptions may evict those pods.
- Changing a PDB does not create spare nodes, CPU, memory, or Kafka quorum.

For this lab, do not force a drain by disabling eviction or deleting controller pods while the cluster is degraded.

### Why 2 KRaft controllers is a bad HA target

Do not scale the controller pool from 3 to 2 for an HA design. Keep 3 controllers:

```yaml
spec:
  replicas: 3
  roles:
    - controller
```

The safe learning design is:

```text
3 KRaft controllers
3 Kafka brokers
3 worker nodes + preferably 1 spare worker
```

---

## 18. If you specifically want a 2-broker learning scenario

This is a capacity experiment, **not the same HA design** as the 3-broker cluster. Before reducing brokers, existing topics/internal topics must be compatible with the smaller broker count. A topic with replication factor 3 cannot maintain 3 replicas on only 2 brokers.

Do not simply change the broker pool to 2 while `sre-events` still has:

```yaml
replicas: 3
```

For a clean 2-broker experiment, use a separate non-production cluster and define topics with replication factor no greater than 2. Keep 3 controllers if you want controller quorum redundancy.

Example broker pool change for a clean lab only:

```yaml
spec:
  replicas: 2
  roles:
    - broker
```

Example topic for that separate 2-broker lab:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: two-broker-events
  namespace: kafka
  labels:
    strimzi.io/cluster: sre-kafka
spec:
  partitions: 4
  replicas: 2
  config:
    min.insync.replicas: 1
```

Do not call `min.insync.replicas: 1` a production HA recommendation. It is only an availability/failure demonstration for a 2-broker lab and changes durability semantics.

---

## 19. Capacity troubleshooting: Pending Kafka broker

Your lab already reproduced this scheduler message:

```text
Insufficient memory
Insufficient cpu
```

Troubleshoot with:

```bash
kubectl describe pod <pending-broker> -n kafka
kubectl get events -n kafka --sort-by=.lastTimestamp
kubectl top nodes
kubectl top pods -n kafka --containers
kubectl describe nodes
```

Check:

1. CPU requests versus allocatable CPU.
2. Memory requests versus allocatable memory.
3. Node cordon state.
4. `workload=data-platform` node affinity.
5. topology spread constraints.
6. PVC scheduling/zone affinity.
7. taints and tolerations.
8. autoscaler/node-pool capacity if configured.

---

## 20. Common troubleshooting commands

```bash
# Kafka/NodePool status
kubectl get kafka,kafkanodepool -n kafka
kubectl describe kafka sre-kafka -n kafka
kubectl describe kafkanodepool brokers -n kafka
kubectl describe kafkanodepool controllers -n kafka

# Pods
kubectl get pods -n kafka -o wide
kubectl describe pod <pod> -n kafka
kubectl logs <pod> -n kafka
kubectl logs <pod> -n kafka --previous

# Operator
kubectl logs deployment/strimzi-cluster-operator -n kafka --tail=200
kubectl logs deployment/strimzi-cluster-operator -n kafka --since=30m | grep -Ei 'sre-kafka|error|exception|invalid|warning'

# Events
kubectl get events -n kafka --sort-by=.lastTimestamp

# Storage
kubectl get pvc -n kafka -o wide
kubectl get pv
kubectl get storageclass

# Scheduling
kubectl get nodes -L workload,node-slot,kubernetes.io/hostname,topology.kubernetes.io/zone
kubectl get pod <pod> -n kafka -o jsonpath='{.spec.affinity}'
echo

# PDB
kubectl get pdb -n kafka
kubectl describe pdb sre-kafka-kafka -n kafka

# Resources
kubectl top nodes
kubectl top pods -n kafka --containers
```

---

## 21. Upgrade model

Treat the two upgrades separately:

```text
Helm -> Strimzi Operator version
Kafka CR -> Apache Kafka version
```

### 21.1 Operator upgrade

Before an operator upgrade, check:

- target Strimzi release notes;
- supported Kubernetes versions;
- CRD/API changes;
- supported Kafka versions;
- documented upgrade path.

Inspect current release:

```bash
helm list -n kafka
helm get values strimzi -n kafka
```

Then use the release-specific documented Helm upgrade path rather than blindly changing to `latest`.

### 21.2 Kafka upgrade

Determine versions supported by the installed Strimzi release. Then update the declared Kafka version in `kafka-cluster.yaml`, for example only when the documented path permits:

```yaml
spec:
  kafka:
    version: <supported-target-version>
```

Apply and monitor:

```bash
kubectl apply -f kafka-cluster.yaml
kubectl get kafka sre-kafka -n kafka -w
kubectl get pods -n kafka -w
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Do not combine an unverified operator upgrade, Kafka upgrade, storage change, and topology change in one production change.

---

## 22. Cleanup

Delete test client/topic first if desired:

```bash
kubectl delete pod kafka-client -n kafka --ignore-not-found
kubectl delete kafkatopic sre-events -n kafka --ignore-not-found
```

Delete Kafka resources:

```bash
kubectl delete kafka sre-kafka -n kafka
kubectl delete kafkanodepool brokers controllers -n kafka
```

Because the NodePools use:

```yaml
deleteClaim: false
```

review retained PVCs/PVs deliberately before deleting data:

```bash
kubectl get pvc -n kafka
kubectl get pv
```

Only delete retained storage when you are certain the data is no longer required.

Uninstall Strimzi:

```bash
helm uninstall strimzi -n kafka
```

Do not delete CRDs casually on a shared cluster because they are cluster-scoped resources.

---

## 23. Enterprise/GitOps repository structure

A practical repository layout for your FluxCD background:

```text
kafka/
├── operator/
│   └── values.yaml
├── cluster/
│   ├── kafka-controllers.yaml
│   ├── kafka-brokers.yaml
│   └── kafka-cluster.yaml
├── topics/
│   └── kafka-topic.yaml
└── README.md
```

Use PR review and Git history for changes to:

- Strimzi version
- Kafka version
- broker/controller replicas
- storage sizes/classes
- listeners/security
- resource requests/limits
- topology and disruption configuration

---

## 24. Interview summary

A concise SRE explanation:

> I installed the Strimzi Cluster Operator with Helm and managed Kafka declaratively through Kafka and KafkaNodePool custom resources. I used KRaft with separate 3-controller and 3-broker pools, persistent Azure Disk CSI volumes, node affinity and hostname topology spreading. I tested broker recreation, topic replication, producer/consumer traffic, PDB behavior, pending-pod resource failures, and node drain recovery. I also learned that three worker nodes provide placement but no spare scheduling capacity during maintenance, so a realistic drain test needs spare capacity while preserving Kafka quorum and topology constraints.

## References

- Microsoft AKS Kafka/Strimzi guidance: https://learn.microsoft.com/en-us/azure/aks/kafka-deploy
- Microsoft AKS Kafka infrastructure guidance: https://learn.microsoft.com/en-us/azure/aks/kafka-infrastructure
- Strimzi documentation: https://strimzi.io/documentation/
- Strimzi configuration/API reference: https://strimzi.io/docs/operators/latest/configuring.html
- Strimzi GitHub examples: https://github.com/strimzi/strimzi-kafka-operator/tree/main/examples/kafka
