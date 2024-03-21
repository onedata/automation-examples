REGISTRY ?= docker.onedata.org
HUB_USER ?= 

ifdef HUB_USER
	IMAGE := ${REGISTRY}/${HUB_USER}/${REPO_NAME}:${TAG}
else
	IMAGE := ${REGISTRY}/${REPO_NAME}:${TAG}
endif


.PHONY: build publish


build:
	docker build . -t ${IMAGE}


publish:
	docker push ${IMAGE}
