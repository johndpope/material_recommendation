source ${PWD}/_project_env.sh
source ${PWD}/scripts/shell_colors.sh

WORKSPACE=$(cd ${PWD}/..; printf "$PWD")


if [[ "${PROJECT_NAME}" == "" ]]; then
    ENV_PATH="${WORKSPACE}/ENV"
else
    ENV_PATH="${WORKSPACE}/ENV_${PROJECT_NAME}"
fi
