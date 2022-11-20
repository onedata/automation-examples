REPO=docker.onedata.org/lambda-calculate-checksum-onedatafs
TAG=v0


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG}
