REPO=docker.onedata.org/lambda-bagit-uploader-archive-destination-rest
TAG=v2


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG} 
