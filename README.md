# Automation examples

Examples of automation lambdas and workflow schemes that can be used in Onedata.

This repository serves two purposes:

1. Provides examples to easily get started with creating your lambdas and workflows.
2. Provides ready-to-use JSON dumps of workflow schemes that can be loaded
   into an automation inventory; just download a JSON of the desired workflow
   scheme onto your disk and use the "Upload JSON" action in the workflows tab.


## Creating lambda Docker image

> This process requires basic knowledge about Python and Docker, and assumes 
  that you have access to a Docker repository where you can push Docker images.

**Lambda Docker image** defines the internal logic of **Automation lambda**.  
To create one, follow these steps:
1. Navigate to the `lambdas/` directory where subdirectories define sample lambda
   functions.
2. Explore specific lambda examples in the `lambdas/` directory to understand how 
   different lambdas are structured and defined - each directory contains:
   - `handler.py`: the definition of the function executed by the lambda. 
   It MUST define the `handle` function:
      ```
      def handle(
         job_batch_request: AtmJobBatchRequest[JobArgs, AtmObject],
         heartbeat_callback: AtmHeartbeatCallback,
      ) -> AtmJobBatchResponse[JobResults]:
         ...
      ```

   - `requirements.txt`: external Python dependencies/libraries needed to define 
   the function. It is recommended to familiarize yourself and utilize 
   [`onedata-lambda-utils` library](https://pypi.org/project/onedata-lambda-utils/), 
   which provides various utilities for writing lambdas (e.g. including the `types` 
   module with documented argument and result types for use in lambdas).

   - `Dockerfile`: specifies the base image that MUST be used to build the Docker 
   image. Also, if your function requires additional non-Python dependencies, 
   this is the place you can install them. To do that, switch the user to ROOT, 
   install your dependencies and lastly switch the user to APP, e.g.:
      ```
      FROM docker.onedata.org/lambda-base-slim:v1

      USER root

      RUN ...

      USER app
      ```
      See the `Dockerfile` in `download-file-mounted` or 
      `detect-file-format-mounted` for concrete examples.

   - `Makefile`: specifies the Docker repository name (REPO_NAME) and tag (TAG), 
   along with other useful commands:
      - `type-check`: performs type-checking on the lambda code using `mypy` tool.

      - `format`: formats the lambda code using `isort` and `black` tools.

      - `black-check`: checks if the lambda code is formatted according to `black` 
      standards.

      - `static-analysis`: conducts static code analysis on the lambda code 
      using `pylint`.

      - `build`: builds the Docker image for the lambda. By default, the image 
      is built in the format `docker.onedata.org/<REPO_NAME>:<TAG>` where 
      `docker.onedata.org` is the private Docker registry for the Onedata team, 
      to which you may not have access. To use a different registry or user, 
      you need to specify/override the `REGISTRY` and/or `HUB_USER` variables 
      either in the Makefile:
         ```makefile
         REGISTRY = docker.io
         HUB_USER = onedata
         REPO_NAME = ...
         TAG = ...
         ```      
         or when running commands from the command line, for example:
         ```console
         make build REGISTRY=docker.io HUB_USER=onedata
         ```

      - `publish`: publishes the Docker image to the specified repository. 
      It uses the same rules for naming the image as `make build`.

3. Create a new directory with the above-mentioned structure (or copy one of 
the existing subdirectories and rework it accordingly).

4. Define function logic in `handler.py`.

5. Build a Docker image.

6. Publish a Docker image.

Now, you can use a built image when defining automation lambda in Onezone.


## Contributing to this repo

To add a new lambda/workflow schema or modify an existing one, follow these steps:

1. Develop using the dev channel (docker.onedata.org - see above), use lambda
   images pushed to this repo for testing on bamboo (you can simply edit the
   workflow JSON dumps to change the lambda docker image).
2. Always update image tags when introducing any changes to a lambda,
   especially when preparing an official image to be pushed (see below).
3. At the end, when the code is ready to be merged, you will have to
   make sure all the images used in workflow schemas are publicly available:
   - all referenced images belong to the docker.io registry
     (you can use `make workflows-ensure-all-used-docker-images-are-public`)
   - all of the referenced images are pushed to the registry
     (you can use `make lambdas-publish-public` - but be sure that the images
     are properly tagged! otherwise, you may overwrite existing ones)
