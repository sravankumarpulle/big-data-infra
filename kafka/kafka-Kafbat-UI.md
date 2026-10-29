# Kafbat UI on AKS - Installation, LoadBalancer Access and IP Whitelisting Runbook

## 1. Purpose

This runbook deploys Kafbat UI on the existing AKS Kafka lab and connects it to the Strimzi-managed Kafka cluster `sre-kafka`.
# disabled user login
Target design:

```text
Allowed client IP: 163.116.214.124/32
              |
              v
      Azure Load Balancer
              |
        Service :80
              |
              v
        Kafbat UI :8080
              |
              v
sre-kafka-kafka-bootstrap:9092
              |
              v
        Kafka brokers
```

The runbook covers:

- prerequisites
- checking the Strimzi bootstrap service
- installing Kafbat UI with Helm
- exposing Kafbat UI using a Kubernetes `LoadBalancer` Service
- restricting the public LoadBalancer to `163.116.214.124/32`
- Kubernetes NetworkPolicy for the UI pod
- validation and troubleshooting
- Helm upgrade and rollback
- cleanup

> Security note: authentication is disabled below only for this controlled lab. Do not expose an unauthenticated Kafka administration UI broadly to the Internet.

---

## 2. Prerequisites

Confirm access to the AKS cluster:

```bash
kubectl cluster-info
kubectl get nodes -o wide
```

Confirm Helm:

```bash
helm version
```

Confirm Kafka is healthy:

```bash
kubectl get kafka -n kafka
kubectl get kafkanodepool -n kafka
kubectl get pods -n kafka -o wide
```

The Kafka cluster should be ready before deploying the UI.

---

## 3. Find the Kafka bootstrap service

```bash
kubectl get svc -n kafka
```

Filter bootstrap services:

```bash
kubectl get svc -n kafka | grep bootstrap
```

For this lab, the expected internal bootstrap endpoint is:

```text
sre-kafka-kafka-bootstrap:9092
```

Verify instead of assuming:

```bash
kubectl get svc sre-kafka-kafka-bootstrap -n kafka
```

The UI and Kafka are deployed in the same `kafka` namespace, so the short Kubernetes service name can be used.

---

## 4. Add the Kafbat UI Helm repository

```bash
helm repo add kafbat-ui https://kafbat.github.io/helm-charts
helm repo update
helm search repo kafbat-ui
```

Inspect chart values before installation:

```bash
helm show values kafbat-ui/kafka-ui > kafka-ui-default-values.yaml
```

Useful checks:

```bash
grep -n -A 30 '^service:' kafka-ui-default-values.yaml
grep -n 'loadBalancerSourceRanges' kafka-ui-default-values.yaml
```

This matters because Helm chart value keys can change between chart versions.

---

## 5. Create `kafka-ui-values.yaml`

```yaml
yamlApplicationConfig:
  kafka:
    clusters:
      - name: sre-kafka
        bootstrapServers: sre-kafka-kafka-bootstrap:9092
        properties: {}

  auth:
    type: disabled

  management:
    health:
      ldap:
        enabled: false

service:
  type: LoadBalancer
  port: 80
  loadBalancerSourceRanges:
    - 163.116.214.124/32
```

Validate the intended allowed source:

```text
163.116.214.124/32
```

`/32` represents one IPv4 address.

If the installed chart does not expose `service.loadBalancerSourceRanges`, keep the UI configuration in Helm and apply the dedicated Service manifest in section 8 instead.

---

## 6. Install Kafbat UI

```bash
helm upgrade --install kafka-ui \
  kafbat-ui/kafka-ui \
  --namespace kafka \
  -f kafka-ui-values.yaml
```

Check Helm:

```bash
helm list -n kafka
helm status kafka-ui -n kafka
```

Check Kubernetes resources:

```bash
kubectl get deployment -n kafka
kubectl get pods -n kafka -o wide
kubectl get svc -n kafka
```

---

## 7. Verify the actual Kafbat pod labels

Never guess the labels before creating a NetworkPolicy.

```bash
kubectl get pods -n kafka --show-labels | grep -E 'kafka-ui|kafbat'
```

Also inspect the Deployment selector:

```bash
kubectl get deployment -n kafka
kubectl get deployment kafka-ui -n kafka -o jsonpath='{.spec.selector.matchLabels}'
echo
```

If your Deployment name is different, replace `kafka-ui` with the real name returned by `kubectl get deployment`.

The NetworkPolicy in this runbook assumes this pod label:

```yaml
app.kubernetes.io/name: kafka-ui
```

If Helm generated another label, change the NetworkPolicy selector to match the actual pod labels.

---

## 8. Dedicated LoadBalancer Service manifest if required

If the chart does not support `service.loadBalancerSourceRanges`, create `kafka-ui-service.yaml`.

First obtain the real Service selector from the Helm-created Service or Deployment:

```bash
kubectl get svc kafka-ui -n kafka -o yaml
```

Example manifest:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-ui
  namespace: kafka
spec:
  type: LoadBalancer
  selector:
    app.kubernetes.io/name: kafka-ui
  ports:
    - name: http
      protocol: TCP
      port: 80
      targetPort: 8080
  loadBalancerSourceRanges:
    - 163.116.214.124/32
```

Server-side validation:

```bash
kubectl apply --dry-run=server -f kafka-ui-service.yaml
```

Apply:

```bash
kubectl apply -f kafka-ui-service.yaml
```

> Prefer managing the Service through Helm values when the chart supports the required field. Avoid having Helm and a separate manifest fight over ownership of the same Service.

---

## 9. Verify the LoadBalancer

```bash
kubectl get svc kafka-ui -n kafka -o wide
```

Watch until an external address is assigned:

```bash
kubectl get svc kafka-ui -n kafka -w
```

Check source-range configuration:

```bash
kubectl get svc kafka-ui \
  -n kafka \
  -o jsonpath='{.spec.loadBalancerSourceRanges}'
echo
```

Expected:

```text
["163.116.214.124/32"]
```

Inspect the complete Service:

```bash
kubectl describe svc kafka-ui -n kafka
```

Get only the external IP:

```bash
kubectl get svc kafka-ui \
  -n kafka \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
echo
```

Open the returned external IP using HTTP from the allowed client network.

---

## 10. Kubernetes NetworkPolicy

Create `kafka-ui-networkpolicy.yaml` after verifying the real pod label.

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: kafka-ui-ingress
  namespace: kafka
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: kafka-ui

  policyTypes:
    - Ingress

  ingress:
    - from:
        - ipBlock:
            cidr: 163.116.214.124/32
      ports:
        - protocol: TCP
          port: 8080
```

Validate:

```bash
kubectl apply --dry-run=server -f kafka-ui-networkpolicy.yaml
```

Apply:

```bash
kubectl apply -f kafka-ui-networkpolicy.yaml
```

Verify:

```bash
kubectl get networkpolicy -n kafka
kubectl describe networkpolicy kafka-ui-ingress -n kafka
```

### Important

The Service-level `loadBalancerSourceRanges` is the primary public-source restriction in this runbook. The NetworkPolicy is defense-in-depth at pod level.

The manifest declares only:

```yaml
policyTypes:
  - Ingress
```

It does not intentionally introduce egress isolation. Kafbat UI still needs to reach Kafka and cluster DNS.

---

## 11. Test UI connectivity to Kafka

Check Kafbat logs:

```bash
kubectl get pods -n kafka | grep -E 'kafka-ui|kafbat'
```

Then:

```bash
kubectl logs deployment/kafka-ui -n kafka --tail=200
```

If the Deployment name differs:

```bash
kubectl get deployment -n kafka
```

Check bootstrap DNS from the UI pod:

```bash
UI_POD=$(kubectl get pods -n kafka \
  -l app.kubernetes.io/name=kafka-ui \
  -o jsonpath='{.items[0].metadata.name}')

echo "$UI_POD"
```

If the container image contains the required utilities, test name resolution/connectivity. Do not assume diagnostic tools such as `nslookup`, `curl`, or `nc` are present in the application image. A separate debug pod can be used instead.

Example debug pod:

```bash
kubectl run network-debug \
  -n kafka \
  --restart=Never \
  --image=busybox:1.36 \
  --command -- sleep 3600
```

DNS test:

```bash
kubectl exec -n kafka network-debug -- \
  nslookup sre-kafka-kafka-bootstrap
```

Cleanup:

```bash
kubectl delete pod network-debug -n kafka
```

---

## 12. Test the IP whitelist

### From the allowed source

The public client source should be:

```text
163.116.214.124
```

Confirm your actual observed public egress IP from your organization's approved method before testing. VPNs, corporate proxies, Secure Web Gateways, and NAT can cause the IP seen by Azure to differ from the workstation's interface address.

Open:

```text
http://<KAFKA_UI_EXTERNAL_IP>
```

### From another source

Try from a network whose public source IP is not `163.116.214.124`. Access should not be allowed by the LoadBalancer source-range restriction.

---

## 13. Useful Kafbat operational commands

```bash
# Helm status
helm status kafka-ui -n kafka
helm get values kafka-ui -n kafka
helm history kafka-ui -n kafka

# Kubernetes workload
kubectl get deployment -n kafka
kubectl get pods -n kafka -o wide
kubectl describe deployment kafka-ui -n kafka

# Service
kubectl get svc kafka-ui -n kafka -o wide
kubectl describe svc kafka-ui -n kafka

# Logs
kubectl logs deployment/kafka-ui -n kafka --tail=200
kubectl logs deployment/kafka-ui -n kafka --since=30m

# Network policies
kubectl get networkpolicy -n kafka
kubectl describe networkpolicy kafka-ui-ingress -n kafka

# Kafka connectivity dependencies
kubectl get svc sre-kafka-kafka-bootstrap -n kafka
kubectl get kafka -n kafka
kubectl get pods -n kafka -o wide

# Events
kubectl get events -n kafka --sort-by=.lastTimestamp
```

---

## 14. Troubleshooting

### UI Service has `<pending>` external IP

```bash
kubectl describe svc kafka-ui -n kafka
kubectl get events -n kafka --sort-by=.lastTimestamp
```

Check the AKS LoadBalancer integration and whether the Service is actually `type: LoadBalancer`.

### Browser cannot connect

Check:

```bash
kubectl get svc kafka-ui -n kafka -o wide
kubectl get endpoints kafka-ui -n kafka
kubectl get pods -n kafka -o wide
```

Then verify:

```bash
kubectl get svc kafka-ui \
  -n kafka \
  -o jsonpath='{.spec.loadBalancerSourceRanges}'
echo
```

Make sure the public source reaching Azure is really `163.116.214.124`.

### UI is running but Kafka cluster is unavailable in UI

Check Kafka:

```bash
kubectl get kafka -n kafka
kubectl get pods -n kafka -o wide
kubectl get svc sre-kafka-kafka-bootstrap -n kafka
```

Check UI logs:

```bash
kubectl logs deployment/kafka-ui -n kafka --tail=200
```

Verify the configured bootstrap endpoint:

```text
sre-kafka-kafka-bootstrap:9092
```

### NetworkPolicy accidentally blocks legitimate access

Confirm which pods it selects:

```bash
kubectl describe networkpolicy kafka-ui-ingress -n kafka
kubectl get pods -n kafka --show-labels | grep -E 'kafka-ui|kafbat'
```

Temporarily deleting a policy changes security posture, so diagnose the selector/source path first rather than deleting controls blindly.

---

## 15. Helm upgrade

Before upgrading:

```bash
helm repo update
helm search repo kafbat-ui/kafka-ui --versions
helm history kafka-ui -n kafka
helm get values kafka-ui -n kafka > kafka-ui-current-values.yaml
```

Inspect the target chart's release notes/default values before changing versions.

Upgrade with the maintained values file:

```bash
helm upgrade kafka-ui \
  kafbat-ui/kafka-ui \
  --namespace kafka \
  -f kafka-ui-values.yaml
```

Verify:

```bash
helm status kafka-ui -n kafka
kubectl rollout status deployment/kafka-ui -n kafka
kubectl get pods -n kafka -o wide
kubectl get svc kafka-ui -n kafka -o wide
```

---

## 16. Helm rollback

View revisions:

```bash
helm history kafka-ui -n kafka
```

Rollback to a known revision:

```bash
helm rollback kafka-ui <REVISION> -n kafka
```

Verify:

```bash
helm status kafka-ui -n kafka
kubectl get pods -n kafka -o wide
```

---

## 17. Change the allowed public IP

Edit `kafka-ui-values.yaml`:

```yaml
service:
  type: LoadBalancer
  port: 80
  loadBalancerSourceRanges:
    - NEW.PUBLIC.IP.ADDRESS/32
```

Also update `kafka-ui-networkpolicy.yaml` if you use its `ipBlock` restriction.

Then apply the Helm-managed configuration:

```bash
helm upgrade kafka-ui \
  kafbat-ui/kafka-ui \
  --namespace kafka \
  -f kafka-ui-values.yaml
```

Apply the NetworkPolicy:

```bash
kubectl apply -f kafka-ui-networkpolicy.yaml
```

Verify both layers afterward.

---

## 18. Cleanup

Delete the NetworkPolicy:

```bash
kubectl delete networkpolicy kafka-ui-ingress -n kafka --ignore-not-found
```

If the Service was manually created outside Helm:

```bash
kubectl delete service kafka-ui -n kafka --ignore-not-found
```

Uninstall Kafbat UI:

```bash
helm uninstall kafka-ui -n kafka
```

Confirm:

```bash
kubectl get pods,svc,networkpolicy -n kafka
```

This cleanup does not intentionally delete the Strimzi Kafka cluster.

---

## 19. Recommended repository layout

```text
kafka-ui/
├── kafka-ui-values.yaml
├── kafka-ui-networkpolicy.yaml
└── README.md
```

If the chart cannot manage source ranges and you deliberately maintain the Service separately:

```text
kafka-ui/
├── kafka-ui-values.yaml
├── kafka-ui-service.yaml
├── kafka-ui-networkpolicy.yaml
└── README.md
```

For GitOps, keep the values and policy in Git and review changes through PRs rather than modifying the live Service manually.

---

## 20. Interview-ready explanation

> I deployed Kafbat UI into the Kafka namespace using its Helm chart and configured it against the Strimzi Kafka bootstrap service. I exposed the UI through an AKS LoadBalancer Service and restricted the public source to a single `/32` IP. I added a Kubernetes ingress NetworkPolicy as defense-in-depth, validated the actual Helm-generated labels before selecting pods, and kept Kafka egress connectivity available. Operationally, I monitor the Deployment, Service, endpoints, NetworkPolicy, application logs, Kafka bootstrap service, and Kubernetes events when troubleshooting access.
