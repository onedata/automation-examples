#!/bin/bash

# List of functions defined locally in this script
local_functions=(
  # update all lambdas tags in makefiles
  # increase all tags by 1: --inc
  # decrease all tags by 1: --dec
  # add additional suffix to the tag e.g.: --inc/--dec -dev
  # function ignores current tag suffix (anything after tag number)
  update_all_lambda_tags_in_makefiles

  # update all lambdas tags in dockerfiles
  # increase all tags by 1: --inc
  # decrease all tags by 1: --dec
  # add additional suffix to the tag e.g.: --inc/--dec -dev
  # function ignores current tag suffix (anything after tag number)
  update_all_lambda_tags_in_dockerfiles

  # update every lambda image in its dump
  # public repo: --public
  # dev repo: --dev
  # functions replaces lambda image in dump with the one in makefile and uses the selected repo
  update_all_lambda_images_in_dumps_with_makefile

  # update every lambda repo in its dockerfile
  # public repo: --public
  # dev repo: --dev
  # functions replaces lambda repo in dockerfile
  update_all_lambda_repos_in_dockerfiles
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
      # current_suffix="${BASH_REMATCH[2]}"

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

update_all_lambda_tags_in_dockerfiles() {
  local action="$1"     # --inc or --dec
  local suffix="$2"     # Optional: e.g., -dev

  for dockerfile in lambdas/*/docker/Dockerfile; do
    if [[ -f "$dockerfile" ]]; then
      local first_line
      first_line=$(head -n1 "$dockerfile")

      # Format: FROM <image>:<tag>
      if [[ "$first_line" =~ ^[[:space:]]*FROM[[:space:]]+.+:[^[:space:]]+ ]]; then
        local current_tag="${first_line##*:}"
        local prefix="${first_line%:*}"

        if [[ "$current_tag" =~ ^v([0-9]+)(-.+)?$ ]]; then
          local version="${BASH_REMATCH[1]}"

          if [[ "$action" == "--inc" ]]; then
            version=$((version + 1))
          elif [[ "$action" == "--dec" ]]; then
            version=$((version - 1))
          fi
          (( version < 0 )) && version=0

          local new_tag="v$version"
          [[ -n "$suffix" ]] && new_tag="${new_tag}${suffix}"

          local new_first_line="${prefix}:${new_tag}"

          awk -v repl="$new_first_line" 'NR==1{print repl; next} {print}' "$dockerfile" > "$dockerfile.tmp" \
            && mv "$dockerfile.tmp" "$dockerfile"

          echo "Updated $dockerfile: ${current_tag} -> ${new_tag}"
        else
          echo "Skipping $dockerfile: unrecognized tag format '$current_tag'"
        fi
      else
        echo "Skipping $dockerfile: first line is not in expected format: FROM <image>:<tag>"
      fi
    else
      echo "Skipping: $dockerfile does not exist or is not a file"
    fi
  done
}

update_all_lambda_repos_in_dockerfiles() {
  local mode="$1"  # --public lub --dev

  if [[ "$mode" != "--public" && "$mode" != "--dev" ]]; then
    echo "Usage: $0 update_all_lambda_repos_in_dockerfiles [--public|--dev]"
    return 1
  fi

  local new_repo=""
  if [[ "$mode" == "--public" ]]; then
    new_repo="onedata"
  else
    new_repo="docker.onedata.org"
  fi

  for dockerfile in lambdas/*/docker/Dockerfile; do
    if [[ -f "$dockerfile" ]]; then
      local first_line
      first_line=$(head -n1 "$dockerfile")

      # Format: FROM <image>:<tag>, where
      # <image> is <repo>/<path>
      if [[ $first_line =~ ^[[:space:]]*FROM[[:space:]]+([^/]+)/([^:]+):([^[:space:]]+) ]]; then
        local current_repo="${BASH_REMATCH[1]}"
        local image_path="${BASH_REMATCH[2]}"
        local tag="${BASH_REMATCH[3]}"

        local new_first_line="FROM ${new_repo}/${image_path}:${tag}"

        awk -v repl="$new_first_line" 'NR==1{print repl; next} {print}' "$dockerfile" > "$dockerfile.tmp" \
          && mv "$dockerfile.tmp" "$dockerfile"

        echo "Updated $dockerfile: ${current_repo}/${image_path}:${tag} -> ${new_repo}/${image_path}:${tag}"
      else
        echo "Skipping $dockerfile: first line is not in expected format: FROM <repo>/<image_path>:<tag>"
      fi
    else
      echo "Skipping: $dockerfile does not exist or is not a file"
    fi
  done
}

# update lamda image in lambda dump with the one provided in Makefile
# and uses the selected public or dev repo
update_lambda_image_in_dump_with_makefile() {
    local mode="$1"           # --public or --dev
    local lambda_name="$2"
    file=lambdas/"$lambda_name"/"$lambda_name".json
    local target_file="${3:-$file}"
    local output_file="${4:-$file}"
    docker_image=$(extract_docker_image_from_makefile lambdas/"$lambda_name"/docker/Makefile)

    if [[ "$mode" != "--public" && "$mode" != "--dev" ]]; then
        echo "Usage: update_lambda_image_in_dump_with_makefile [--public|--dev] <lambda_name> [target_file] [output_file]"
        return 1
    fi

    local repo=""
    if [[ "$mode" == "--public" ]]; then
        repo="onedata"
    else
        repo="docker.onedata.org"
    fi

    # Replace all .dockerImage fields with the new image
    jq --arg new_image "$repo/$docker_image" \
       'walk(if type == "object" and has("dockerImage") then .dockerImage = $new_image else . end)' \
       "$target_file" > "$output_file.tmp" && mv "$output_file.tmp" "$output_file"

    echo "Replaced dockerImage with onedata/$docker_image in $output_file"
}

update_all_lambda_images_in_dumps_with_makefile() {
  local mode="$1"  # --public or --dev

  if [[ "$mode" != "--public" && "$mode" != "--dev" ]]; then
    echo "Usage: update_all_lambda_images_in_dumps_with_makefile [--public|--dev]"
    return 1
  fi

  for lambda_dir in lambdas/*/; do
    if [[ -d "$lambda_dir" ]]; then
      local base_lambda_name
      base_lambda_name=$(basename "$lambda_dir")
      update_lambda_image_in_dump_with_makefile "$mode" "$base_lambda_name"
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