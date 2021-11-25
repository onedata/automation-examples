REPO=docker.onedata.org/checksum-counting-oneclient
TAG=v7

.PHONY: checksum-counting-oneclient


checksum-counting-oneclient:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}

