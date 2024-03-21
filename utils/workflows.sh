#!/bin/bash

print_error() {
    printf "\033[0;31mError:\033[0m %s\n" "$1"
}

extract_docker_images_from_dump() {
    local file="$1"
    jq -r '.. | objects | select(has("dockerImage")) | .dockerImage' "$file" | sort | uniq
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
        *)
            echo "Unknown function: $1"
            ;;
    esac
fi
