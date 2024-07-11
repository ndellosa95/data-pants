#!/usr/bin/env bash zsh

# Run this script to generate lockfiles for the 5 most recent minor versions.

SEMVER_REGEX='"release_(?<major>[0-9]+)\\.(?<minor>[0-9]+)\\.(?<patch>[0-9]+)(?<suffix>.*)"'
RELEASES=$( 
    curl 'https://api.github.com/repos/pantsbuild/pants/releases?per_page=50' | 
    jq 'map({"name": .name, "semver": (.name | capture('"$SEMVER_REGEX"') | (.major + "." + .minor))})' 
)
MINOR_VERSIONS=$( jq 'map(.semver) | unique | sort | .[-5:]' <<< "$RELEASES" )
PANTS_VERSIONS=($( jq -r --argjson semvers "$MINOR_VERSIONS" 'map(select(.semver as $semver | $semvers | index($semver))) | group_by(.semver) | map(first) | .[].name' <<< "$RELEASES" ))
for PV in "${PANTS_VERSIONS[@]}"; do
    read -p "Regenerate lockfile for Pants version ${PV#release_}? (Y/n)" -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        PANTS_VERSION="${PV#release_}" PANTS_PYTHON_RESOLVES='{"pants-plugins": "ci-lockfiles/'"$PV"'.lock"}' pants generate-lockfiles --resolve='pants-plugins'
    fi
done
