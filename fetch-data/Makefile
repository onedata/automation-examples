REPO=docker.onedata.org
COLLECT_FETCH_INFO_TAG=v5
COLLECT_FETCH_FILES_TAG=v5

.PHONY: all collect-fetch-info fetch-files


all: collect-fetch-info fetch-files


collect-fetch-info:
	docker build ./collect-fetch-info -t ${REPO}/collect-fetch-info:${COLLECT_FETCH_INFO_TAG}
	docker push ${REPO}/collect-fetch-info:${COLLECT_FETCH_INFO_TAG}

fetch-files:
	docker build ./fetch-files -t ${REPO}/fetch-files:${COLLECT_FETCH_FILES_TAG}
	docker push ${REPO}/fetch-files:${COLLECT_FETCH_FILES_TAG}


