REPO=docker.onedata.org/file-mime-format-detection
TAG=v1

.PHONY: file-mime-format-detection


file-mime-format-detection:
	docker build . -t ${REPO}:${TAG}
	docker push ${REPO}:${TAG}

