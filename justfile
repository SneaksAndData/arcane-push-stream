CLUSTER_NAME := "arcane-push-stream"
IMAGE_NAME := "arcane-push-stream"

default:
    @just --list

build:
    sbt assembly

clean:
    sbt clean

test:
    sbt test

coverage:
    sbt clean coverage test coverageReport

run:
    sbt run 2>&1 | tspin

check:
    sbt scalafmtCheckAll

docker-build version="latest":
    GITHUB_TOKEN=$(gh auth token) docker build --secret id=github_token,env=GITHUB_TOKEN -f .container/Dockerfile -t {{ IMAGE_NAME }}:{{ version }} .

[doc("Run in kind: rebuild and load image, upgrade chart")]
docker-up: docker-build
    kind load docker-image {{ IMAGE_NAME }}:latest --name {{ CLUSTER_NAME }}
    helm upgrade --install arcane-ingestion .helm -f .helm/values.yaml -f .helm/values-dev.yaml

[doc("Create kind cluster, build docker image and install helm chart")]
up: docker-build && info
    kind create cluster --name {{ CLUSTER_NAME }}
    @for i in 1 2 3; do \
        kubectl cluster-info --context kind-{{ CLUSTER_NAME }} >/dev/null 2>&1 && echo "cluster ready" && exit 0; \
        echo "waiting for cluster... ($i/3)"; \
        sleep 1; \
    done; \
    echo "cluster did not become ready in time"; exit 1
    kind load docker-image {{ IMAGE_NAME }}:latest --name {{ CLUSTER_NAME }}
    helm upgrade --install arcane-ingestion .helm -f .helm/values.yaml -f .helm/values-dev.yaml
    @echo ""
    @echo "Set kubectl context:"
    @printf "\033[0;38;5;40mkubectl use-context %s\033[0m\n" "{{ CLUSTER_NAME }}"

[doc("delete dev kind cluster")]
stop:
    kind delete cluster --name {{ CLUSTER_NAME }}

[doc("(Re-)create dev environment")]
fresh: stop up

[doc("Show dev environment status")]
info:
    #!/usr/bin/env bash
    echo -e "\033[1;4;97mstatus:\033[0m"
    echo -n "  kind cluster:         "
    count=$(kind get clusters | grep -c "{{ CLUSTER_NAME }}")
    if [ "$count" -gt 0 ]; then
        echo "👌"
    else
        echo "❌"
    fi
    echo -n "  container image:      "
    if docker image inspect {{ IMAGE_NAME }}:latest >/dev/null 2>&1; then
        echo "👌"
    else
        echo "❌"
    fi
    echo -n "  image loaded to kind: "
    if docker exec {{ CLUSTER_NAME }}-control-plane crictl images -q docker.io/library/{{ IMAGE_NAME }}:latest 2>/dev/null | grep -q .; then
        echo "👌"
    else
        echo "❌"
    fi
    echo -n "  helm release:         "
    status=$(helm status arcane-ingestion 2>/dev/null | awk '/^STATUS:/ {print $2}')
    if [ -n "$status" ]; then
        echo "👌 ($status)"
    else
        echo "❌"
    fi
