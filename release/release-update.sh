#!/bin/sh
# Build, tag and push docker image for the release cycle.
# This is the job run in Jenkins as part of jobs 1_release-start and 2_release-update in
#   Jenkins folder TapisJava->2_Release-Service-<service>
# RC version must be the first and only argument to the script
# Existing docker login is used for push
# Docker image is created with a unique tag: tapis/<IMG_NAME>-<VER>-<YYYYmmddHHMM>-<COMMIT>
#   - other tags are created and updated as appropriate
#

PrgName=$(basename "$0")
USAGE="Usage: $PrgName { <tapis_version> <commit_hash> <rc_version> }"

SVC_NAME="files"
REPO="tapis"
BUILD_DIR="../api/target"
VER=$1
GIT_COMMIT=$2
RC_VER=$3

# service contains three docker images as part of deployment
IMG1="tapis-files"
IMG2="tapis-files-workers"
IMG2="tapis-files-migrations"

# Check number of arguments
if [ $# -ne 3 ]; then
  echo $USAGE
  exit 1
fi

# Determine absolute path to location from which we are running
#  and change to that directory.
export RUN_DIR=$(pwd)
export PRG_RELPATH=$(dirname "$0")
cd "$PRG_RELPATH"/. || exit
export PRG_PATH=$(pwd)

# Make sure service has been built
if [ ! -d "$BUILD_DIR" ]; then
  echo "Build directory missing. Please build. Directory: $BUILD_DIR"
  exit 1
fi

# Set variables used for build
TAG_UNIQ1="${REPO}/${IMG1}:${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT}"
TAG_UNIQ2="${REPO}/${IMG2}:${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT}"
TAG_UNIQ3="${REPO}/${IMG3}:${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT}"
TAG_RC1="${REPO}/${IMG1}:${VER}-rc${RC_VER}"
TAG_RC2="${REPO}/${IMG2}:${VER}-rc${RC_VER}"
TAG_RC3="${REPO}/${IMG3}:${VER}-rc${RC_VER}"
TAG_DEV1="${REPO}/${IMG1}:dev"
TAG_DEV2="${REPO}/${IMG2}:dev"
TAG_DEV3="${REPO}/${IMG3}:dev"

# If branch name is UNKNOWN or empty as might be the case in a jenkins job then
#   set it to GIT_BRANCH. Jenkins jobs should have this set in the env.
if [ -z "$GIT_BRANCH_LBL" -o "x$GIT_BRANCH_LBL" = "xUNKNOWN" ]; then
  GIT_BRANCH_LBL=$(echo "$GIT_BRANCH" | awk -F"/" '{print $2}')
fi

# Build images from Dockerfiles
echo "Building local images"
echo "  VER=        ${VER}"
echo "  GIT_BRANCH_LBL= ${GIT_BRANCH_LBL}"
echo "  GIT_COMMIT_LBL= ${GIT_COMMIT_LBL}"

docker build -f ./deploy/Dockerfile -t "${TAG_UNIQ1}" .
docker build -f ./deploy/Dockerfile.workers -t "${TAG_UNIQ2}" .
docker build -f ./deploy/Dockerfile.migrations -t "${TAG_UNIQ3}" .

echo "Creating RC and DEV image tags"
docker tag "$TAG_UNIQ1" "$TAG_RC1"
docker tag "$TAG_UNIQ2" "$TAG_RC2"
docker tag "$TAG_UNIQ3" "$TAG_RC3"
docker tag "$TAG_UNIQ1" "$TAG_DEV1"
docker tag "$TAG_UNIQ2" "$TAG_DEV2"
docker tag "$TAG_UNIQ3" "$TAG_DEV3"

echo "Pushing images and tags to docker hub."
# NOTE: Use current login. Jenkins job does login
docker push "$TAG_UNIQ1"
docker push "$TAG_UNIQ2"
docker push "$TAG_UNIQ3"
docker push "$TAG_RC1"
docker push "$TAG_RC2"
docker push "$TAG_RC3"
docker push "$TAG_DEV1"
docker push "$TAG_DEV2"
docker push "$TAG_DEV3"

cd "$RUN_DIR"
