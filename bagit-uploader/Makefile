REPO=docker.onedata.org
TAG=v18

.PHONY: all validate unpack-data unpack-fetch fetch-files register-metadata calculate-checksum register-metadata-from-data

all: validate unpack-data unpack-fetch fetch-files register-metadata calculate-checksum register-metadata-from-data

validate:
	docker build ./validate -t ${REPO}/bagit-uploader-validate:${TAG}
	docker push ${REPO}/bagit-uploader-validate:${TAG}


unpack-data:
	docker build ./unpack-data -t ${REPO}/bagit-uploader-unpack-data:${TAG}
	docker push ${REPO}/bagit-uploader-unpack-data:${TAG}


unpack-fetch:
	docker build ./unpack-fetch -t ${REPO}/bagit-uploader-unpack-fetch:${TAG}
	docker push ${REPO}/bagit-uploader-unpack-fetch:${TAG}


fetch-files:
	docker build ./fetch-files -t ${REPO}/bagit-uploader-fetch-files:${TAG}
	docker push ${REPO}/bagit-uploader-fetch-files:${TAG}


register-metadata:
	docker build ./register-metadata -t ${REPO}/bagit-uploader-register-metadata:${TAG}
	docker push ${REPO}/bagit-uploader-register-metadata:${TAG}


register-metadata-from-data:
	docker build ./register-metadata-from-data -t ${REPO}/bagit-uploader-register-metadata-from-data:${TAG}
	docker push ${REPO}/bagit-uploader-register-metadata-from-data:${TAG}


calculate-checksum:
	docker build ./calculate-checksum -t ${REPO}/bagit-uploader-calculate-checksum:${TAG}
	docker push ${REPO}/bagit-uploader-calculate-checksum:${TAG}
