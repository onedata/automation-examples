REPO=docker.onedata.org/lambda-bagit-uploader-parse-fetch-file-mounted
TAG=v1


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG}