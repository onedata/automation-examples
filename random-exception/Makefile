REPO=docker.onedata.org/random-exception
TAG=v8

.PHONY: random-exception

random-exception:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}
