##
## Black reformatting
##

black-format:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  docker.onedata.org/python_static_analyser:v5 black . 


black-check:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  docker.onedata.org/python_static_analyser:v5 black . --check
