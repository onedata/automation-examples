REPO=docker.onedata.org
COLLECT_FETCH_INFO_TAG=v6

.PHONY: all collect-fetch-info fetch-files


all: collect-fetch-info


collect-fetch-info:
	docker build ./collect-fetch-info -t ${REPO}/collect-fetch-info:${COLLECT_FETCH_INFO_TAG}
	docker push ${REPO}/collect-fetch-info:${COLLECT_FETCH_INFO_TAG}


