REPO=docker.onedata.org
TAG=v4

.PHONY: all collect-fetch-info fetch-files


all: collect-fetch-info fetch-files


collect-fetch-info:
	docker build ./collect-fetch-info -t ${REPO}/collect-fetch-info:${TAG}
	docker push ${REPO}/collect-fetch-info:${TAG}

fetch-files:
	docker build ./fetch-files -t ${REPO}/fetch-files:${TAG}
	docker push ${REPO}/fetch-files:${TAG}


