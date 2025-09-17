#!/bin/bash

# List of functions defined locally in this script
local_functions=(
  update_all_lambda_tags_in_makefiles
  update_all_lambda_images_in_dumps_with_makefile
)

extract_docker_image_from_makefile() {
    local file="$1"
    version=$(grep -m 1 TAG "$file" | sed 's/^.*= //g')
    image=$(grep -m 1 REPO_NAME "$file" | sed 's/^.*= //g')
    echo "$image":"$version"
}

update_all_lambda_tags_in_makefiles() {
  local action="$1"     # --inc or --dec
  local suffix="$2"     # Optional: e.g., -dev

  for makefile in lambdas/*/docker/Makefile; do
    if [[ -f "$makefile" ]]; then
      current_tag=$(grep -E '^TAG\s*=' "$makefile" | sed -E 's/^TAG\s*=\s*//')

      # Extract numeric version (vN) and optional suffix
      if [[ "$current_tag" =~ ^v([0-9]+)(-.+)?$ ]]; then
        version="${BASH_REMATCH[1]}"
#        current_suffix="${BASH_REMATCH[2]}"

        # Increment or decrement version
        if [[ "$action" == "--inc" ]]; then
          version=$((version + 1))
        elif [[ "$action" == "--dec" ]]; then
          version=$((version - 1))
        fi

        # Prevent version from going below 0
        if (( version < 0 )); then
          version=0
        fi

        new_tag="v$version"
        if [[ -n "$suffix" ]]; then
          new_tag="${new_tag}${suffix}"
        fi

        sed -i -E "s/^(TAG\s*=\s*).*/\1$new_tag/" "$makefile"
        echo "Updated $makefile -> $new_tag"
      else
        echo "Skipping $makefile: Unrecognized TAG format '$current_tag'"
      fi
    else
      echo "Skipping: $makefile not found or not a regular file"
    fi
  done
}

# update lamda image in lambda dump with the one provided in Makefile
update_lambda_image_in_dump_with_makefile() {
    local lambda_name="$1"
    file=lambdas/"$lambda_name"/"$lambda_name".json
    local target_file="${2:-$file}"
    local output_file="${3:-$file}"
    docker_image=$(extract_docker_image_from_makefile lambdas/"$lambda_name"/docker/Makefile)

    # Replace all .dockerImage fields with the new image
    # if needed you can change here onedata -> dokcer.onedata.org for devs purposes
    jq --arg new_image "dokcer.onedata.org/$docker_image" \
       'walk(if type == "object" and has("dockerImage") then .dockerImage = $new_image else . end)' \
       "$target_file" > "$output_file.tmp" && mv "$output_file.tmp" "$output_file"

    echo "Replaced dockerImage with onedata/$docker_image in $output_file"
}

update_all_lambda_images_in_dumps_with_makefile() {
  for lamda_name in lambdas/*/; do
  if [[ -d "$lamda_name" ]]; then
    base_lamda_name=$(basename "$lamda_name")
    update_lambda_image_in_dump_with_makefile "$base_lamda_name"
  fi
done
}


if [[ $# -ge 1 ]]; then
  func="$1"
  shift
  if [[ " ${local_functions[*]} " =~ ${func} ]]; then
    "$func" "$@"
  else
    echo "Error: function '$func' is not defined in this script"
    exit 1
  fi
else
  echo "Usage: $0 <function_name> [args...]"
  exit 1
fi