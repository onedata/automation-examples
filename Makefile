STATIC_ANALYSER_IMAGE := "docker.onedata.org/python_static_analyser:v5"

##
## Black reformatting
##

black-format:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) black . 


black-check:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) black . --check
		
##
## Isort import sorting
##

isort-format:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) isort -rc . 


isort-check:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) isort -rc . --check
	
##
## Static analysis
##

static-analysis:	
	docker run --rm -i -v /home/rwidzisz/automation-examples:/tmp/automation-examples $(STATIC_ANALYSER_IMAGE) pylint /tmp/automation-examples --rcfile /tmp/rc_file		
