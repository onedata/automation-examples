REPO=docker.onedata.org/file-format-detection
TAG=v0

.PHONY: file-format-detection


file-format-detection:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}

