REPO=docker.onedata.org/lambda-detect-file-mime-type-mounted
TAG=v1


.PHONY: build publish


build:
	docker build . -t ${REPO}:${TAG}


publish:
	docker push ${REPO}:${TAG}
