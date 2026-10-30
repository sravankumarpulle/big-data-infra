# Kafka Client Access, SCRAM/TLS, Users, ACLs and Endpoints on Strimzi

## Purpose

This runbook documents an enterprise-style client-access model for the existing `sre-kafka` cluster on AKS using Strimzi, including:

- Separate Kafka identities for producers and consumers
- SCRAM-SHA-512 authentication
- TLS encryption and certificate trust
- Kafka ACL authorization
- Internal AKS access across namespaces
- External access and access from another AKS cluster
- Producer and consumer CLI examples
- Consumer groups and `group.id`
- Kubernetes Secrets for Kafka credentials
- The distinction between `KafkaUser`, a Kafka-side service identity, and Kubernetes `ServiceAccount`

> In this SCRAM design, `KafkaUser` is the Kafka-side application/service identity. You do not need an additional Kafka service-account object for producers and consumers.

---

## 1. Architecture

```text
                         sre-kafka
                             |
             +---------------+---------------+
             |                               |
       INTERNAL LISTENER              EXTERNAL LISTENER
       SASL/SCRAM + TLS               SASL/SCRAM + TLS
       ClusterIP                      LoadBalancer/private LB
       port 9093                      port 9094
             |                               |
      AKS applications                 External clients
      other namespaces                / other AKS clusters
             |                               |
       KafkaUser                       KafkaUser
             |                               |
         Kafka ACLs                     Kafka ACLs
```

Example Kafka identities:

```text
orders-producer
orders-consumer
sre-admin
analytics-consumer
```

Avoid sharing one highly privileged Kafka identity across every application.

---

## 2. KafkaUser versus Kubernetes ServiceAccount

For this design:

```text
KafkaUser = Kafka client/application identity
```

Example:

```text
orders-api
    |
KafkaUser: orders-producer
    |
SCRAM credential
    |
Kafka ACL
```

A Kubernetes `ServiceAccount` is a Kubernetes identity and is not required for Kafka SCRAM authentication itself.

There are authentication architectures based on service-account identity, but they are alternatives to this SCRAM-based design rather than an additional mandatory object.

---

## 3. Configure secure Kafka listeners

Update the relevant portions of `kafka-cluster.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: sre-kafka
  namespace: kafka
spec:
  kafka:
    version: 4.2.0

    listeners:
      - name: internal
        port: 9093
        type: internal
        tls: true
        authentication:
          type: scram-sha-512

      - name: external
        port: 9094
        type: loadbalancer
        tls: true
        authentication:
          type: scram-sha-512

    authorization:
      type: simple

    config:
      default.replication.factor: 3
      min.insync.replicas: 2
      offsets.topic.replication.factor: 3
      transaction.state.log.replication.factor: 3
      transaction.state.log.min.isr: 2

  entityOperator:
    topicOperator: {}
    userOperator: {}
```

Validate against the CRDs actually installed in the cluster:

```bash
kubectl explain kafka.spec.kafka.listeners
kubectl explain kafka.spec.kafka.listeners.authentication
kubectl explain kafka.spec.kafka.authorization
kubectl apply --dry-run=server -f kafka-cluster.yaml
```

Apply and monitor:

```bash
kubectl apply -f kafka-cluster.yaml
kubectl get kafka sre-kafka -n kafka -w
```

---

## 4. Create the orders topic

`orders-topic.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: orders
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
kubectl apply --dry-run=server -f orders-topic.yaml
kubectl apply -f orders-topic.yaml
kubectl get kafkatopic -n kafka
```

---

## 5. Producer KafkaUser

`orders-producer.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: orders-producer
  namespace: kafka
  labels:
    strimzi.io/cluster: sre-kafka
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: orders
          patternType: literal
        operations:
          - Write
          - Describe
        host: "*"
```

Validate and apply:

```bash
kubectl apply --dry-run=server -f orders-producer.yaml
kubectl apply -f orders-producer.yaml
kubectl get kafkauser orders-producer -n kafka
```

Resulting intent:

```text
orders-producer -> WRITE/DESCRIBE -> orders
```

---

## 6. Consumer KafkaUser

`orders-consumer.yaml`:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: orders-consumer
  namespace: kafka
  labels:
    strimzi.io/cluster: sre-kafka
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: orders
          patternType: literal
        operations:
          - Read
          - Describe
        host: "*"

      - resource:
          type: group
          name: orders-processing
          patternType: literal
        operations:
          - Read
        host: "*"
```

Validate and apply:

```bash
kubectl apply --dry-run=server -f orders-consumer.yaml
kubectl apply -f orders-consumer.yaml
kubectl get kafkauser orders-consumer -n kafka
```

Resulting intent:

```text
orders-consumer -> READ/DESCRIBE -> orders
orders-consumer -> group READ -> orders-processing
```

---

## 7. Inspect generated SCRAM Secrets

```bash
kubectl get secret orders-producer -n kafka
kubectl get secret orders-consumer -n kafka
```

Inspect key names without printing credentials:

```bash
kubectl get secret orders-producer -n kafka -o jsonpath='{.data}' | jq 'keys'
kubectl get secret orders-consumer -n kafka -o jsonpath='{.data}' | jq 'keys'
```

Lab-only password retrieval:

```bash
kubectl get secret orders-producer   -n kafka   -o jsonpath='{.data.password}'   | base64 -d
echo
```

```bash
kubectl get secret orders-consumer   -n kafka   -o jsonpath='{.data.password}'   | base64 -d
echo
```

Do not expose these values in CI/CD logs or shared terminals.

---

## 8. Obtain the Strimzi cluster CA

Find CA-related Secrets:

```bash
kubectl get secret -n kafka | grep ca
```

For the standard cluster CA Secret naming used by this lab:

```bash
kubectl get secret sre-kafka-cluster-ca-cert   -n kafka   -o jsonpath='{.data.ca\.crt}'   | base64 -d > ca.crt
```

Inspect:

```bash
openssl x509   -in ca.crt   -noout   -subject   -issuer   -dates
```

Kafka clients using the Strimzi-generated listener certificate must trust the CA that signed it.

---

## 9. Producer client.properties

Example `producer.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="orders-producer" password="REPLACE_FROM_SECRET";
ssl.truststore.type=PEM
ssl.truststore.certificates=/etc/kafka/secrets/ca.crt
```

Do not commit a real password into Git.

---

## 10. Consumer client.properties

Example `consumer.properties`:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="orders-consumer" password="REPLACE_FROM_SECRET";
ssl.truststore.type=PEM
ssl.truststore.certificates=/etc/kafka/secrets/ca.crt
group.id=orders-processing
```

The consumer-group ID is Kafka configuration. It is not another network endpoint.

---

## 11. Same AKS cluster, different namespace

Kafka namespace:

```text
kafka
```

Producer namespace:

```text
payments
```

Consumer namespace:

```text
order-processing
```

Use the internal Kafka service through cross-namespace Kubernetes DNS:

```text
sre-kafka-kafka-bootstrap.kafka:9093
```

Fully qualified form:

```text
sre-kafka-kafka-bootstrap.kafka.svc.cluster.local:9093
```

Both producer and consumer use this bootstrap listener. Their Kafka identity and ACLs determine what they can do.

---

## 12. Provision producer credentials into the payments namespace

Create namespace:

```bash
kubectl create namespace payments
```

For a lab, retrieve the generated password:

```bash
PRODUCER_PASSWORD=$(kubectl get secret orders-producer   -n kafka   -o jsonpath='{.data.password}'   | base64 -d)
```

Create the application Secret:

```bash
kubectl create secret generic kafka-producer-credentials   -n payments   --from-literal=username=orders-producer   --from-literal=password="$PRODUCER_PASSWORD"   --from-file=ca.crt=ca.crt
```

Remove the shell variable:

```bash
unset PRODUCER_PASSWORD
```

Verify metadata:

```bash
kubectl get secret kafka-producer-credentials -n payments
```

In an enterprise implementation, replace manual credential copying with the organization's controlled secret-delivery mechanism.

---

## 13. Producer CLI client pod

`producer-client.yaml`:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: orders-producer-client
  namespace: payments
spec:
  containers:
    - name: kafka-client
      image: quay.io/strimzi/kafka:1.3.0-kafka-4.2.0
      command:
        - sleep
        - "36000"
      env:
        - name: KAFKA_USERNAME
          valueFrom:
            secretKeyRef:
              name: kafka-producer-credentials
              key: username
        - name: KAFKA_PASSWORD
          valueFrom:
            secretKeyRef:
              name: kafka-producer-credentials
              key: password
      volumeMounts:
        - name: kafka-ca
          mountPath: /etc/kafka/secrets
          readOnly: true
  volumes:
    - name: kafka-ca
      secret:
        secretName: kafka-producer-credentials
        items:
          - key: ca.crt
            path: ca.crt
```

Apply:

```bash
kubectl apply -f producer-client.yaml
kubectl get pod orders-producer-client -n payments
```

Enter the pod:

```bash
kubectl exec -it -n payments orders-producer-client -- bash
```

Build runtime properties inside the pod:

```bash
cat > /tmp/client.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="$KAFKA_USERNAME" password="$KAFKA_PASSWORD";
ssl.truststore.type=PEM
ssl.truststore.certificates=/etc/kafka/secrets/ca.crt
EOF
```

Produce:

```bash
bin/kafka-console-producer.sh   --bootstrap-server sre-kafka-kafka-bootstrap.kafka:9093   --topic orders   --producer.config /tmp/client.properties
```

Sample records:

```text
order-10001
order-10002
order-10003
```

---

## 14. Internal consumer from another namespace

Create namespace:

```bash
kubectl create namespace order-processing
```

Provision credentials for the lab:

```bash
CONSUMER_PASSWORD=$(kubectl get secret orders-consumer   -n kafka   -o jsonpath='{.data.password}'   | base64 -d)
```

```bash
kubectl create secret generic kafka-consumer-credentials   -n order-processing   --from-literal=username=orders-consumer   --from-literal=password="$CONSUMER_PASSWORD"   --from-file=ca.crt=ca.crt
```

```bash
unset CONSUMER_PASSWORD
```

The consumer uses:

```properties
bootstrap.servers=sre-kafka-kafka-bootstrap.kafka:9093
group.id=orders-processing
```

CLI pattern:

```bash
bin/kafka-console-consumer.sh   --bootstrap-server sre-kafka-kafka-bootstrap.kafka:9093   --topic orders   --group orders-processing   --consumer.config /tmp/client.properties
```

---

## 15. Consumer groups do not have separate endpoints

Incorrect mental model:

```text
orders-processing:9093
```

Correct model:

```text
Kafka bootstrap endpoint
sre-kafka-kafka-bootstrap.kafka:9093
              |
              v
            orders
              |
              v
       orders-processing
           group.id
```

Producer:

```text
KafkaUser: orders-producer
Topic: orders
Permission: WRITE
```

Consumer:

```text
KafkaUser: orders-consumer
Topic: orders
Permission: READ
Group ID: orders-processing
```

---

## 16. Discover the external Kafka endpoint

The external listener is configured as:

```yaml
type: loadbalancer
```

List generated services:

```bash
kubectl get svc -n kafka
```

Read listener status from Strimzi:

```bash
kubectl get kafka sre-kafka   -n kafka   -o jsonpath='{.status.listeners}'
echo
```

Use the address published by the cluster. Do not invent an external hostname.

---

## 17. External producer CLI

Assume, only as an example, that your actual Strimzi status reports an approved reachable endpoint named `kafka.example.internal:9094`.

`producer.properties`:

```properties
bootstrap.servers=kafka.example.internal:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="orders-producer" password="SECRET";
ssl.truststore.type=PEM
ssl.truststore.certificates=/secure/path/ca.crt
```

CLI:

```bash
kafka-console-producer.sh   --bootstrap-server kafka.example.internal:9094   --topic orders   --producer.config producer.properties
```

Replace the example endpoint with the address actually published by Strimzi.

---

## 18. Consumer from another AKS cluster

A client in AKS cluster 2 cannot use the Kubernetes `ClusterIP` DNS name belonging to AKS cluster 1.

Architecture:

```text
AKS Cluster 2
      |
      | approved network path
      v
AKS Cluster 1 external/private Kafka listener
      |
      v
sre-kafka
```

Example `consumer.properties` using a placeholder endpoint:

```properties
bootstrap.servers=kafka.example.internal:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="orders-consumer" password="SECRET";
ssl.truststore.type=PEM
ssl.truststore.certificates=/secure/path/ca.crt
group.id=orders-processing
```

CLI:

```bash
kafka-console-consumer.sh   --bootstrap-server kafka.example.internal:9094   --topic orders   --group orders-processing   --consumer.config consumer.properties
```

---

## 19. Same consumer group from multiple Kubernetes clusters

Multiple clients can connect to the same Kafka cluster using the same consumer `group.id`:

```text
                Kafka Cluster

                  orders
             P0 P1 P2 P3 P4 P5
                    |
        group.id=orders-processing
              /                        /                      AKS-2            AKS-3
       consumer-1       consumer-2
```

For Kafka, group membership is associated with the Kafka cluster/group ID rather than the Kubernetes cluster where a consumer happens to run.

If two consumers connect to completely separate Kafka clusters, their group state is separate even if both use the string `orders-processing`.

---

## 20. TLS trust

SCRAM authenticates the Kafka client identity. TLS encrypts the connection and allows the client to validate the server certificate.

Lab model:

```text
Strimzi internal CA
        |
      ca.crt
        |
Kafka client trust
```

Enterprise listener model can instead use an organization-approved certificate/PKI, provided the client trusts the CA that signed the listener certificate.

---

## 21. Network-security layers

Kafka authentication should not replace network controls.

```text
Network access
VPN / VNet / firewall / source CIDR
             |
             v
TLS encryption / server trust
             |
             v
SCRAM authentication
             |
             v
Kafka ACL authorization
             |
             v
Topic / consumer group
```

For external access, validate the intended private/public exposure and listener source restrictions with the Strimzi version installed in the cluster.

---

## 22. Enterprise identity pattern

```text
Kafka
│
├── orders-producer
│    authentication = SCRAM
│    topic = orders
│    ACL = WRITE
│
├── orders-consumer
│    authentication = SCRAM
│    topic = orders
│    ACL = READ
│    group = orders-processing
│
├── invoice-producer
│    authentication = SCRAM
│    topic = invoices
│    ACL = WRITE
│
├── invoice-consumer
│    authentication = SCRAM
│    topic = invoices
│    ACL = READ
│    group = invoice-processing
│
└── sre-admin
     privileged operational permissions
```

The principle is separate identities and least privilege rather than one shared account for all workloads.

---

## 23. Interview-ready answers

### Do producers and consumers require Kafka service accounts?

Kafka clients require an authenticated Kafka identity, but in this Strimzi/SCRAM design you do not create an extra Kafka service-account object. Separate `KafkaUser` resources serve as the producer/consumer application identities. Each receives its own SCRAM credential and least-privilege ACLs.

### How does another namespace access Kafka?

Use the internal bootstrap DNS, for example:

```text
sre-kafka-kafka-bootstrap.kafka:9093
```

The application also needs TLS trust, SCRAM credentials and the required Kafka ACLs.

### How does another AKS cluster access Kafka?

The source cluster cannot use the destination cluster's `ClusterIP` DNS service directly. It needs an approved network-reachable listener such as the configured external/private listener, then uses TLS/SCRAM and Kafka ACLs in the same way as other clients.

### What is the consumer-group endpoint?

There is no separate consumer-group endpoint. Consumers connect to a Kafka bootstrap listener and configure `group.id`. Kafka manages group membership and offsets internally.

---

## 24. References

- Strimzi configuration reference: https://strimzi.io/docs/operators/latest/configuring.html
- Strimzi overview: https://strimzi.io/docs/operators/latest/overview.html
- Strimzi Helm chart: https://artifacthub.io/packages/helm/strimzi/strimzi-kafka-operator
- Listener certificate guidance: https://github.com/strimzi/strimzi-kafka-operator/blob/main/documentation/modules/security/proc-installing-certs-per-listener.adoc
- Kafka access using load balancers reference supplied during the lab: https://github.com/IBM/strimzi-kafka-operator/blob/main/documentation/modules/security/proc-accessing-kafka-using-loadbalancers.adoc
