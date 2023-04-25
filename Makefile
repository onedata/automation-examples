STATIC_ANALYSER_IMAGE := "docker.onedata.org/python_static_analyser:v5"

##
## Black reformatting
##

black-format:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) black . 


black-check:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) black . --check
