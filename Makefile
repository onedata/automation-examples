##
## Lambdas
##

include lambdas/code_style_common.mk

LAMBDA_DIRS := $(foreach dir,$(wildcard lambdas/*),$(if $(wildcard $(dir)/handler.py),$(dir)))

define foreach_lambda
	for lambda_dir in $(LAMBDA_DIRS); do \
		$(MAKE) -C $$lambda_dir $1 || exit 1; \
	done
endef

# Formatting works recursively by default so aliasing simply works
lambdas-format: format
lambdas-black-check: black-check
lambdas-static-analysis: static-analysis

lambdas-type-check:	
	$(call foreach_lambda,type-check)

lambdas-build-dev:
	$(call foreach_lambda,build)

lambdas-publish-dev:
	$(call foreach_lambda,publish)

lambdas-build-public:
	$(call foreach_lambda,build REGISTRY=docker.io HUB_USER=onedata)

lambdas-publish-public:
	$(call foreach_lambda,publish REGISTRY=docker.io HUB_USER=onedata)
