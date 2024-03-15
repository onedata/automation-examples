STATIC_ANALYSER_IMAGE := "docker.onedata.org/python_static_analyser:v6"

.PHONY: format

##
## Formatting
##

format:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) isort -rc .
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) black --fast . 


black-check:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) black . --check || (echo "Code failed Black format checking. Please run 'make format' before commiting your changes. "; exit 1)
		
	
##
## Static analysis
##

static-analysis:	
	docker run --rm -i -v `pwd`:`pwd` -w `pwd`  $(STATIC_ANALYSER_IMAGE) pylint . --rcfile=.pylintrc --recursive=y


##
## Type checking
##

type-check:
	docker run --rm -i -v `pwd`:`pwd` -w `pwd` $(STATIC_ANALYSER_IMAGE) sh -c "pip install -qq -r requirements.txt && mypy handler.py --ignore-missing-imports"
