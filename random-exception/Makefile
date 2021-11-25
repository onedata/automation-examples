REPO=docker.onedata.org/random-exception
TAG=v1

.PHONY: random-exception

random-exception:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}
