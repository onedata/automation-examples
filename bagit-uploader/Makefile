REPO=docker.onedata.org
VALIDATE_TAG=v9
UNPACK_DATA_TAG=v3
UNPACK_FETCH_TAG=v10

REGISTER_METADATA_TAG=v4
REGISTER_METADATA_FROM_DATA_TAG=v4
TAG=v16

all: infer-destination validate unpack-data unpack-fetch fetch-files register-metadata calculate-checksum register-metadata-from-data archive-destination
.PHONY: all validate unpack-data unpack-fetch fetch-files register-metadata calculate-checksum register-metadata-from-data raise-exception


infer-destination:
	docker build ./infer-destination -t ${REPO}/bagit-uploader-infer-destination:${TAG}
	docker push ${REPO}/bagit-uploader-infer-destination:${TAG}
all: validate unpack-data unpack-fetch fetch-files register-metadata calculate-checksum register-metadata-from-data raise-exception

validate:
	docker build ./validate -t ${REPO}/bagit-uploader-validate:${VALIDATE_TAG}
	docker push ${REPO}/bagit-uploader-validate:${VALIDATE_TAG}


unpack-data:
	docker build ./unpack-data -t ${REPO}/bagit-uploader-unpack-data:${UNPACK_DATA_TAG}
	docker push ${REPO}/bagit-uploader-unpack-data:${UNPACK_DATA_TAG}


unpack-fetch:
	docker build ./unpack-fetch -t ${REPO}/bagit-uploader-unpack-fetch:${UNPACK_FETCH_TAG}
	docker push ${REPO}/bagit-uploader-unpack-fetch:${UNPACK_FETCH_TAG}


fetch-files:
	docker build ./fetch-files -t ${REPO}/bagit-uploader-fetch-files:${TAG}
	docker push ${REPO}/bagit-uploader-fetch-files:${TAG}


register-metadata:
	docker build ./register-metadata -t ${REPO}/bagit-uploader-register-metadata:${REGISTER_METADATA_TAG}
	docker push ${REPO}/bagit-uploader-register-metadata:${REGISTER_METADATA_TAG}


register-metadata-from-data:
	docker build ./register-metadata-from-data -t ${REPO}/bagit-uploader-register-metadata-from-data:${REGISTER_METADATA_FROM_DATA_TAG}
	docker push ${REPO}/bagit-uploader-register-metadata-from-data:${REGISTER_METADATA_FROM_DATA_TAG}


calculate-checksum:
	docker build ./calculate-checksum -t ${REPO}/bagit-uploader-calculate-checksum:${TAG}
	docker push ${REPO}/bagit-uploader-calculate-checksum:${TAG}


archive-destination:
	docker build ./archive-destination -t ${REPO}/bagit-uploader-archive-destination:${TAG}
	docker push ${REPO}/bagit-uploader-archive-destination:${TAG}

raise-exception:
	docker build ./raise-exception -t ${REPO}/bagit-uploader-raise-exception:${TAG}
	docker push ${REPO}/bagit-uploader-raise-exception:${TAG}