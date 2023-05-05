#!/usr/bin/env bash
#********************************************************#
#         Python Virtual Environment setup
#              create_python_venv.sh
#                 April 2023
#********************************************************#

#--------------------------------------------------------------------
# Prints usage of script, non-zero exit in case of incorrect usage
# Parameter: N/A
# Returns: N/A
#--------------------------------------------------------------------
scriptUsage() {
  logError "Usage: ${SCRIPT_NAME} [./create_python_venv.sh -n VIRTUAL_ENV_NAME]"
  logError "Do not use 'sh' shell to run the script; use 'bash' or ./create_python_venv.sh <args>" 1>&2
  exit 1;
}

#-----------------------------------------------------------------------
# Checks if python3 exists otherwise exit with non-zero
# Parameter: N/A
# Returns: 0 if python3 exist else exit with non-zero
#-----------------------------------------------------------------------
python3Exists() {
  logWarn "Checking if python3 is installed on machine"
  if ! which python3; then
    logError "python3 is not installed in the machine, please install python3 as base to create virtual environments on top of base python"
    exit 1;
  fi
  log "python3 already installed on machine"
  return 0;
}

#-----------------------------------------------------------------------
# Checks if pip tool exists otherwise downloads from internet to install
# Exit with non-zero in case of poor or no internet connection
# Parameter: N/A
# Returns: N/A
#-----------------------------------------------------------------------
pipExists() {
  logWarn "Checking if pip tool is installed on machine or attempt to download from internet & install"
  if ! pip --version; then
    if ! curl https://bootstrap.pypa.io/get-pip.py --output get-pip.py; then
      logError "Error downloading file from the internet; check your internet connection & proxy settings"
      exit 1;
    else
      log "Downloaded get-pip.py successfully"
      if ! python get-pip.py; then
        logError "Error installing pip, check logs"
        exit 1;
      fi
      log "pip installed successfully, upgrading"
      python3 -m pip install --upgrade pip
      return 0
    fi
  else
    log "pip tool already available on machine, upgrading"
    python3.9 -m pip install --upgrade pip
    return 0
  fi
}

#--------------------------------------------------------------------
# Installs virtualenvwrapper
# Exit with non-zero in case of any error during installation,
# Parameter: N/A
# Returns: 0 if installation is successful, non-zero exit otherwise
#--------------------------------------------------------------------
installVEnvWrapper() {
  mkdir -p "$HOME/python_venvs/"
  if ! pip install virtualenvwrapper; then
    logError "Error installing virtualenvwrapper using pip; check logs & check your internet connection & proxy"
    exit 1;
  fi
  return 0
}

#--------------------------------------------------------------------
# Creates python virtual environment by given name
# Exit with non-zero in case of any error during creation,
# Parameter: virtualEnvName: name of the virtual environment to create
# Returns: N/A
#--------------------------------------------------------------------
createVirtualEnv() {
  local virtualEnvName
  virtualEnvName=$(echo "$1" | xargs) #xargs is to trim
  local python3FullPath
  python3FullPath=$(which python3)
  export VIRTUALENVWRAPPER_PYTHON="${python3FullPath}"
  export WORKON_HOME="$HOME/python_venvs/"
  export PROJECT_HOME="${HOME_DIRECTORY}/../"
  logDebug "Using below environment variables & path for virtual environment ${virtualEnvName}"
  logDebug "VIRTUALENVWRAPPER_PYTHON=${VIRTUALENVWRAPPER_PYTHON}"
  logDebug "WORKON_HOME=${WORKON_HOME}"

  source virtualenvwrapper.sh

  rmvirtualenv "${virtualEnvName}"

  if ! mkvirtualenv -a "${HOME_DIRECTORY}/../" -p "${python3FullPath}" "${virtualEnvName}";then
    logError "Error creating virtual environment ${virtualEnvName}"
    exit 1;
  fi
}

#------------------------------------------------------------------------------
# installs required packages for virtual environment given in requirements.txt
# Exit with non-zero in case of any error during installation,
# Parameter: virtualEnvName: name of the virtual environment to create
# Returns: N/A
#------------------------------------------------------------------------------
installRequiredPackages() {
  local virtualEnvName
  virtualEnvName=$(echo "$1" | xargs) #xargs is to trim
  workon "${virtualEnvName}"
  cdproject
  pip install -r requirements.txt
  # pip freeze > requirements.txt
  python3 -m pip install --upgrade pip
  # source activate
}

#************************************************************************
#
#                      MAIN SCRIPTS STARTS HERE
#
#************************************************************************

# Execute ./create_python_venv.sh -n hello-fresh-data-engg

# Read initial variables
HOST_NAME=`hostname`
USER=`whoami`
SCRIPT_NAME=$(basename "$0")
HOME_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${HOME_DIRECTORY}" || exit # exit in case cd fails; very rare
source "${HOME_DIRECTORY}/common_functions.sh"

export SETUPTOOLS_USE_DISTUTILS=stdlib

# trap interrupts 0 SIGHUP SIGINT SIGQUIT SIGABRT SIGALRM SIGTERM
trap interrupt 1 2 3 6 14 15
trap cleanup 0

while getopts ":n:" arg; do
	case "${arg}" in
    n)
      VENV_NAME=${OPTARG}
      ;;
		*)
			scriptUsage
			;;
	esac
done
shift $((OPTIND-1))

if [[ -z ${VENV_NAME} ]]; then
  logError "Empty virtual environment name"
  scriptUsage
fi

mkdir -p "${HOME_DIRECTORY}/../logs/bash/"
LOG_FILE="${HOME_DIRECTORY}/../logs/bash/log-python-venv-setup-${VENV_NAME}-$(date +%F-%H.%M.%S).log"
# Global log redirect
exec &> >(while read -r line; do printf '%s %s\n' "$(date -Iseconds): EXECUTION-LOG - $line"; done | tee -a "${LOG_FILE}" )

log "Executing $SCRIPT_NAME on $HOST_NAME with arguments"

if python3Exists && pipExists; then
  installVEnvWrapper
  createVirtualEnv "${VENV_NAME}"
  installRequiredPackages "${VENV_NAME}"
fi

exit 0;
