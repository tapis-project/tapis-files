#!/bin/sh
# Build and optionally push docker image for service
# This is the job run in Jenkins as part of job in TapisJava->3_ManualBuildDeploy
# Environment name must be passed in as first argument
# Existing docker login is used for push
# Docker image is created with a unique tag: tapis/tapis-<SVC_NAME>-<ENV>-<VER>-<COMMIT>-<YYYYmmddHHMM>
#   - other tags are created and updated as appropriate
#
# Env var TAPIS_DEPLOY_MANUAL may be set to "true" to indicate it is a manual deployment and the
#   image should also be tagged with $ENV

PrgName=$(basename "$0")

USAGE="Usage: $PrgName { dev staging prod } [ -push ]"

SVC_NAME="files"
SVC_NAME2="tapis-${SVC_NAME}"
SVC_NAME2_W="${SVC_NAME2}-workers"
SVC_NAME2_A="${SVC_NAME2}-assigner"
REPO="tapis"

BUILD_DIR=../api/target
ENV=$1

# Check number of arguments
if [ $# -lt 1 -o $# -gt 2 ]; then
  echo $USAGE
  exit 1
fi

# Check that env name is valid
if [ "$ENV" != "dev" -a "$ENV" != "staging" -a "$ENV" != "prod" ]; then
  echo $USAGE
  exit 1
fi

# Check second arg
if [ $# -eq 2 -a "x$2" != "x-push" ]; then
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

# Copy Dockerfile to build dir
# cp Dockerfile $BUILD_DIR

# Move to the build directory
cd $BUILD_DIR || exit

# Set variables used for build
VER=$(cat classes/tapis.version)
GIT_BRANCH_LBL=$(awk '{print $1}' classes/git.info)
GIT_COMMIT_LBL=$(awk '{print $2}' classes/git.info)
TAG_UNIQ="${REPO}/${SVC_NAME2}:${ENV}-${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT_LBL}"
TAGW_UNIQ="${REPO}/${SVC_NAME2_W}:${ENV}-${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT_LBL}"
TAGA_UNIQ="${REPO}/${SVC_NAME2_A}:${ENV}-${VER}-$(date +%Y%m%d%H%M)-${GIT_COMMIT_LBL}"
TAG_ENV="${REPO}/${SVC_NAME2}:${ENV}"
TAGW_ENV="${REPO}/${SVC_NAME2_W}:${ENV}"
TAGA_ENV="${REPO}/${SVC_NAME2_A}:${ENV}"
TAG_LATEST="${REPO}/${SVC_NAME2}:latest"
TAGW_LATEST="${REPO}/${SVC_NAME2_W}:latest"
TAGA_LATEST="${REPO}/${SVC_NAME2_A}:latest"
TAG_LOCAL="${REPO}/${SVC_NAME2}:dev_local"
TAGW_LOCAL="${REPO}/${SVC_NAME2_W}:dev_local"
TAGA_LOCAL="${REPO}/${SVC_NAME2_A}:dev_local"

# If branch name is UNKNOWN or empty as might be the case in a jenkins job then
#   set it to GIT_BRANCH. Jenkins jobs should have this set in the env.
if [ -z "$GIT_BRANCH_LBL" -o "x$GIT_BRANCH_LBL" = "xUNKNOWN" ]; then
  GIT_BRANCH_LBL=$(echo "$GIT_BRANCH" | awk -F"/" '{print $2}')
fi

# Build images from Dockerfiles
echo "Building local image using primary tag: $TAG_UNIQ"
echo "  ENV=        ${ENV}"
echo "  VER=        ${VER}"
echo "  GIT_BRANCH_LBL= ${GIT_BRANCH_LBL}"
echo "  GIT_COMMIT_LBL= ${GIT_COMMIT_LBL}"

# Move to the top level directory to build docker images
cd ${PRG_PATH}/.. || exit
docker build -f ./deploy/Dockerfile \
   --label VER="${VER}" --label GIT_COMMIT="${GIT_COMMIT_LBL}" --label GIT_BRANCH="${GIT_BRANCH_LBL}" \
    -t "${TAG_UNIQ}" .
RET_CODE=$?
if [ $RET_CODE -ne 0 ]; then
  echo "======================================================================"
  echo "Docker build for tapis-files failed. Exit code: $RET_CODE"
  echo "Exiting ..."
  echo "======================================================================"
  exit $RET_CODE
fi

echo "Building local image using primary tag: $TAGW_UNIQ"
docker build -f ./deploy/Dockerfile.workers \
   --label VER="${VER}" --label GIT_COMMIT="${GIT_COMMIT_LBL}" --label GIT_BRANCH="${GIT_BRANCH_LBL}" \
    -t "${TAGW_UNIQ}" .

echo "Building local image using primary tag: $TAGA_UNIQ"
docker build -f ./deploy/Dockerfile.assigner \
   --label VER="${VER}" --label GIT_COMMIT="${GIT_COMMIT_LBL}" --label GIT_BRANCH="${GIT_BRANCH_LBL}" \
    -t "${TAGA_UNIQ}" .

# Create other tags for remote repo
echo "Creating image for local testing user tag: $TAG_LOCAL"
docker tag "$TAG_UNIQ" "$TAG_LOCAL"
docker tag "$TAGW_UNIQ" "$TAGW_LOCAL"
docker tag "$TAGA_UNIQ" "$TAGA_LOCAL"

# Push to remote repo
if [ "x$2" = "x-push" ]; then
  if [ "$ENV" = "prod" ]; then
    echo "Creating third image tag for prod env: $TAG_LATEST"
    docker tag "$TAG_UNIQ" "$TAG_LATEST"
    docker tag "$TAGW_UNIQ" "$TAGW_LATEST"
    docker tag "$TAGA_UNIQ" "$TAGA_LATEST"
  fi
  echo "Pushing images to docker hub."
  # NOTE: Use current login. Jenkins job does login
  docker push "$TAG_UNIQ"
  RET_CODE=$?
  if [ $RET_CODE -ne 0 ]; then
    echo "======================================================================"
    echo "Docker push for $TAG_UNIQ failed. Exit code: $RET_CODE"
    echo "Exiting ..."
    echo "======================================================================"
    exit $RET_CODE
  fi
  echo "********** Pushed image: $TAG_UNIQ"

  docker push "$TAGW_UNIQ"
  if [ "x$TAPIS_DEPLOY_MANUAL" = "xtrue" ]; then
    echo "Creating ENV image tag: $TAG_ENV"
    docker tag "$TAG_UNIQ" "$TAG_ENV"
    docker tag "$TAGW_UNIQ" "$TAGW_ENV"
    docker tag "$TAGA_UNIQ" "$TAGA_ENV"
    docker push "$TAG_ENV"
    docker push "$TAGW_ENV"
    docker push "$TAGA_ENV"
  fi
  if [ "$ENV" = "prod" ]; then
    docker push "$TAG_LATEST"
    docker push "$TAGW_LATEST"
    docker push "$TAGA_LATEST"
  fi
fi
cd "$RUN_DIR"
