REPO=docker.onedata.org/hello-world-file
TAG=v2

.PHONY: hello-world-file

hello-world-file:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}
