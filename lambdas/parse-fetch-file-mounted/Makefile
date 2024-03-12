REPO=docker.onedata.org/lambda-parse-fetch-file-mounted
TAG=v2


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG} 
