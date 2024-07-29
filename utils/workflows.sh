#!/bin/bash

print_error() {
    printf "\033[0;31mError:\033[0m %s\n" "$1"
}

extract_docker_images_from_dump() {
    local file="$1"
    jq -r '.. | objects | select(has("dockerImage")) | .dockerImage' "$file" | sort | uniq
}

extract_docker_image_from_makefile() {
    local file="$1"
    version=$(grep -m 1 TAG "$file" | sed 's/^.*= //g')
    image=$(grep -m 1 REPO_NAME "$file" | sed 's/^.*= //g')
    echo "$image":"$version"
}

ensure_all_used_docker_images_are_public() {
    local workflow_dump="$1"
    local dev_prefix="${2:-docker.onedata.org}"
    local public_prefix="${3:-onedata}"

    docker_images=$(jq -r '.. | objects | select(has("dockerImage") and (.dockerImage | tostring | startswith("'"$dev_prefix"'"))) | .dockerImage' "$workflow_dump")
    for image in $docker_images; do
        new_image=$(echo "$image" | sed 's/'"$dev_prefix"'/'"$public_prefix"'/g')
        sed -i "s|$image|$new_image|g" "$workflow_dump"
    done
}

assert_only_public_docker_images_are_used() {
    local all_used_docker_images=("$@")
    echo "Checking if only public docker images are used..."
    for image in "${all_used_docker_images[@]}"; do
        if [[ "$image" != "onedata/"* ]]; then
            print_error "Found a non-public docker image in one of the workflow schemas - $image"
            return 1
        fi
    done
    echo "All used docker images are public."
}

assert_all_used_docker_images_are_published() {
    local all_used_docker_images=("$@")
    echo "Checking if all used images are published..."
    for image in "${all_used_docker_images[@]}"; do
        if ! docker manifest inspect "$image" &> /dev/null; then
            print_error "Found an unpublished docker image in one of the workflow schemas - $image"
            return 1
        fi
    done
    echo "All used docker images are published."
}

assert_lambda_image_is_used_in_lambda_dump() {
    local file="$1"
    local lambda_image="$2"
    image_in_dump=$(extract_docker_images_from_dump "$file")
    if [[ "$image_in_dump" == onedata/"$lambda_image" ]]; then
      echo 0
    else
      print_error "image in lambda dump $image_in_dump differs from image in Makefile onedata/$lambda_image" >&2
      echo 1
    fi
}

assert_all_lambda_images_are_used_in_workflows() {
    local all_images_used_in_workflows=("$@")
    local all_lambda_images=()
    local verified=true

    LAMBDA_NAMES=$(find lambdas -maxdepth 1 -type d | cut -d '/' -f 2 -s)

    for lambda_name in $LAMBDA_NAMES; do
        docker_image=$(extract_docker_image_from_makefile lambdas/"$lambda_name"/docker/Makefile)
        is_used_in_lambda_dump=$(assert_lambda_image_is_used_in_lambda_dump lambdas/"$lambda_name"/"$lambda_name".json "$docker_image")
        if [[ "$is_used_in_lambda_dump" == 1 ]]; then
          verified=false
        fi
        all_lambda_images+=($docker_image)
    done

    for lambda_image in "${all_lambda_images[@]}"; do
        if [[ ! $(echo "${all_images_used_in_workflows[@]}" | fgrep -w $lambda_image) ]]
        then
          print_error "image $lambda_image is not used in any workflow"
          verified=false
        fi
    done

    for workflow_image in "${all_images_used_in_workflows[@]}"; do
        # remove dev/public registry prefix
        workflow_image=$(echo $workflow_image | cut -d "/" -f 2)
        if [[ ! $(echo "${all_lambda_images[@]}" | fgrep -w $workflow_image) ]]
        then
          print_error "image $workflow_image used in workflow is obsolete - update to the newest"
          verified=false
        fi
    done

    if [[ $verified == true ]]; then
      echo "Every lambda image in its newest version is used in a workflow"
      return 0
    fi

    return 1
}


WORKFLOW_DUMPS=$(find workflows -type f -name '*.json')

ALL_USED_DOCKER_IMAGES=()
for workflow_dump in $WORKFLOW_DUMPS; do
    docker_images=$(extract_docker_images_from_dump "$workflow_dump")
    ALL_USED_DOCKER_IMAGES+=($docker_images)
done

if [ "$#" -gt 0 ]; then
    case "$1" in
        ensure_all_used_docker_images_are_public)
            shift
            for workflow_dump in $WORKFLOW_DUMPS; do
                ensure_all_used_docker_images_are_public "$workflow_dump" "$@"
            done
            ;;
        assert_only_public_docker_images_are_used)
            assert_only_public_docker_images_are_used "${ALL_USED_DOCKER_IMAGES[@]}"
            ;;
        assert_all_used_docker_images_are_published)
            assert_all_used_docker_images_are_published "${ALL_USED_DOCKER_IMAGES[@]}"
            ;;
        assert_all_lambda_images_are_used_in_workflows)
            assert_all_lambda_images_are_used_in_workflows "${ALL_USED_DOCKER_IMAGES[@]}"
            ;;
        *)
            echo "Unknown function: $1"
            ;;
    esac
fi
