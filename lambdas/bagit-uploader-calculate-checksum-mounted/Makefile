REPO=docker.onedata.org/lambda-bagit-uploader-calculate-checksum-mounted
TAG=v1


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG}