REPO=docker.onedata.org/checksum-counting-rest
TAG=v1


.PHONY: checksum-counting-rest


checksum-counting-rest:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}

