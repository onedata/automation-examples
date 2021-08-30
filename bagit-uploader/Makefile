REPO=docker.onedata.org

INFER_DESTINATION_TAG=v3
VALIDATE_TAG=v9
UNPACK_DATA_TAG=v3
UNPACK_FETCH_TAG=v11
FETCH_FILES_TAG=v24
REGISTER_METADATA_TAG=v4
REGISTER_METADATA_FROM_DATA_TAG=v4
CALCULATE_CHECKSUM_TAG=v5
CONVERT_FILES_TAG=v9
ARCHIVE_DESTINATION_TAG=v6
ARCHIVE_DESTINATION_DM_CERN_TAG=v6


.PHONY: all validate unpack-data unpack-fetch fetch-files register-metadata calculate-checksum register-metadata-from-data infer-destination archive-destination convert-files archive-destination-dm-cern


infer-destination:
	docker build ./infer-destination -t ${REPO}/bagit-uploader-infer-destination:${INFER_DESTINATION_TAG}
	docker push ${REPO}/bagit-uploader-infer-destination:${INFER_DESTINATION_TAG}

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
	docker build ./fetch-files -t ${REPO}/bagit-uploader-fetch-files:${FETCH_FILES_TAG}
	docker push ${REPO}/bagit-uploader-fetch-files:${FETCH_FILES_TAG}


register-metadata:
	docker build ./register-metadata -t ${REPO}/bagit-uploader-register-metadata:${REGISTER_METADATA_TAG}
	docker push ${REPO}/bagit-uploader-register-metadata:${REGISTER_METADATA_TAG}


register-metadata-from-data:
	docker build ./register-metadata-from-data -t ${REPO}/bagit-uploader-register-metadata-from-data:${REGISTER_METADATA_FROM_DATA_TAG}
	docker push ${REPO}/bagit-uploader-register-metadata-from-data:${REGISTER_METADATA_FROM_DATA_TAG}


calculate-checksum:
	docker build ./calculate-checksum -t ${REPO}/bagit-uploader-calculate-checksum:${CALCULATE_CHECKSUM_TAG}
	docker push ${REPO}/bagit-uploader-calculate-checksum:${CALCULATE_CHECKSUM_TAG}


convert-files:
	docker build ./convert-files -t ${REPO}/bagit-uploader-convert-files:${CONVERT_FILES_TAG}
	docker push ${REPO}/bagit-uploader-convert-files:${CONVERT_FILES_TAG}


archive-destination:
	docker build ./archive-destination -t ${REPO}/bagit-uploader-archive-destination:${ARCHIVE_DESTINATION_TAG}
	docker push ${REPO}/bagit-uploader-archive-destination:${ARCHIVE_DESTINATION_TAG}

archive-destination-dm-cern:
	docker build ./archive-destination-dm-cern -t ${REPO}/bagit-uploader-archive-destination-dm-cern:${ARCHIVE_DESTINATION_TAG}
	docker push ${REPO}/bagit-uploader-archive-destination-dm-cern:${ARCHIVE_DESTINATION_DM_CERN_TAG}
