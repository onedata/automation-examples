##
## Lambdas
##

include lambdas/code_style_common.mk

LAMBDA_DIRS := $(foreach dir,$(wildcard lambdas/*),$(if $(wildcard $(dir)/docker/handler.py),$(dir)/docker))

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


##
## Lambdas management
##

SCRIPT_LAMBDA_MANAGEMENT := ./utils/manage_lambdas.sh
SUFFIX ?=  # suffix to set in a lambda image after a tag

lambdas-inc-makefile-tags:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_tags_in_makefiles --inc $(SUFFIX)

lambdas-dec-makefile-tags:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_tags_in_makefiles --dec $(SUFFIX)

lambdas-inc-dockerfile-tags:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_tags_in_dockerfiles --inc $(SUFFIX)

lambdas-dec-dockerfile-tags:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_tags_in_dockerfiles --dec $(SUFFIX)

lambdas-update-dockerfiles-public:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_repos_in_dockerfiles --public

lambdas-update-dockerfiles-dev:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_repos_in_dockerfiles --dev

lambdas-update-dumps-public:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_images_in_dumps_with_makefile --public

lambdas-update-dumps-dev:
	$(SCRIPT_LAMBDA_MANAGEMENT) update_all_lambda_images_in_dumps_with_makefile --dev


##
## Workflows
##

workflows-ensure-all-used-docker-images-are-public:
	@./utils/workflows.sh ensure_all_used_docker_images_are_public

workflows-assert-only-public-docker-images-are-used:
	@./utils/workflows.sh assert_only_public_docker_images_are_used

workflows-assert-all-used-docker-images-are-published:
	@./utils/workflows.sh assert_all_used_docker_images_are_published

workflows-assert-all-lambda-images-are-used-in-workflows:
	@./utils/workflows.sh assert_all_lambda_images_are_used_in_workflows
