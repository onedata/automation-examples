REPO=docker.onedata.org/checksum-counting-cdmi
TAG=v8

.PHONY: checksum-counting-cdmi


checksum-counting-cdmi:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}

