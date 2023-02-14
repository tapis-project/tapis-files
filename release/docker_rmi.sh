#!/bin/sh
# Remove docker images created during a run of docker_build.sh
# This job run in Jenkins as part of job TapisJava->3_ManualBuildDeploy->files
# Environment name must be passed in as first argument
# Existing docker login is used for push

PrgName=$(basename "$0")

USAGE="Usage: $PrgName { dev staging prod }"

SVC_NAME="files"
SVC_NAME2="tapis-${SVC_NAME}"
SVC_NAME2_W="${SVC_NAME2}-workers"
REPO="tapis"

BUILD_DIR=../api/target
ENV=$1

# Check number of arguments
if [ $# -ne 1 ]; then
  echo $USAGE
  exit 1
fi

# Check that env name is valid
if [ "$ENV" != "dev" -a "$ENV" != "staging" -a "$ENV" != "prod" ]; then
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

# Move to the build directory
cd $BUILD_DIR || exit

# Set variables used for build
VER=$(cat classes/tapis.version)
GIT_BRANCH_LBL=$(awk '{print $1}' classes/git.info)
GIT_COMMIT_LBL=$(awk '{print $2}' classes/git.info)
TAG_UNIQ="${REPO}/${SVC_NAME2}:${ENV}-${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT_LBL}"
TAGW_UNIQ="${REPO}/${SVC_NAME2_W}:${ENV}-${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT_LBL}"
TAG_ENV="${REPO}/${SVC_NAME2}:${ENV}"
TAGW_ENV="${REPO}/${SVC_NAME2_W}:${ENV}"
TAG_LATEST="${REPO}/${SVC_NAME2}:latest"
TAGW_LATEST="${REPO}/${SVC_NAME2_W}:latest"
TAG_LOCAL="${REPO}/${SVC_NAME2}:dev_local"
TAGW_LOCAL="${REPO}/${SVC_NAME2_W}:dev_local"

# If branch name is UNKNOWN or empty as might be the case in a jenkins job then
#   set it to GIT_BRANCH. Jenkins jobs should have this set in the env.
if [ -z "$GIT_BRANCH_LBL" -o "x$GIT_BRANCH_LBL" = "xUNKNOWN" ]; then
  GIT_BRANCH_LBL=$(echo "$GIT_BRANCH" | awk -F"/" '{print $2}')
fi

# Remove docker images
echo "Removing docker images based on primary tags: $TAG_UNIQ" and "$TAGW_UNIQ"
echo "  ENV=        ${ENV}"
echo "  VER=        ${VER}"
echo "  GIT_BRANCH_LBL= ${GIT_BRANCH_LBL}"
echo "  GIT_COMMIT_LBL= ${GIT_COMMIT_LBL}"
docker rmi "${TAG_UNIQ}"
docker rmi "${TAG_LOCAL}"
docker rmi "${TAG_LATEST}"
docker rmi "${TAG_ENV}"
docker rmi "${TAGW_UNIQ}"
docker rmi "${TAGW_LOCAL}"
docker rmi "${TAGW_LATEST}"
docker rmi "${TAGW_ENV}"
cd "$RUN_DIR"
