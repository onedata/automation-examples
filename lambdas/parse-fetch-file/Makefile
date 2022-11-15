REPO=docker.onedata.org/parse-fetch-file
TAG=v2


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG} 
