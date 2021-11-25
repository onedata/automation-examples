REPO=docker.onedata.org/in-out
TAG=v1

.PHONY: in-out

in-out:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}
