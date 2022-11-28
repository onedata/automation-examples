REPO=docker.onedata.org/lambda-calculate-checksum-rest
TAG=v1


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG} 
