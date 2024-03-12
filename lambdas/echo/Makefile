REPO=docker.onedata.org/lambda-echo
TAG=v2


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG} 
