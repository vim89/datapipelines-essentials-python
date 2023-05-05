#!/usr/bin/env bash
#********************************************************#
#        Common bash reusable functions
#              library_functions.sh
#                 April 2023
#********************************************************#

#*********************************************************
#                      Comprehensive Logging
#*********************************************************

#--------------------------------------------------------------------
# Prints a log statement
# Parameter: (message) (level: DEBUG,INFO,ERROR,AUDIT)
# Returns: N/A
#--------------------------------------------------------------------
log(){
	local msg=$1
	local lvl=$2
	if [ -z "${lvl}" ]; then
		lvl="INFO"
	fi

	## 0=default; 31=red; 33=yellow, 93=light yellow; 34=blue
	# shellcheck disable=SC2155
	# shellcheck disable=SC2034
	local lts=$(date +%FT%T.%3N)
	case "${lvl}" in
		("ERROR")
		>&2 echo -e "\e[31m${lvl}" $$ "-" "${msg}\e[0m"
		;;
		("WARN")
		echo -e "\e[93m${lvl}" $$ "-" "${msg}\e[0m"
		;;
		("AUDIT")
		echo -e "\e[34m${lvl}" $$ "-" "${msg}\e[0m"
		isCheckRequired=true
		;;
		("DEBUG")
		echo -e "\e[33m${lvl}" $$ "-" "${msg}\e[0m"
		# shellcheck disable=SC2034
		isCheckRequired=true
		;;
		(*) echo -e "\e[0m"$$ "-" "${msg}"
		return 1
		;;
	esac
}

#--------------------------------------------------------------------
# Prints an error
# Parameter: Error message
# Returns: N/A
#--------------------------------------------------------------------
logError(){
	log "$1" "ERROR"
}

#--------------------------------------------------------------------
# Prints a warn message
# Parameter: Error message
# Returns: N/A
#--------------------------------------------------------------------
logWarn(){
	log "$1" "WARN"
}

#--------------------------------------------------------------------
# Prints an audit message
# Parameter: Error message
# Returns: N/A
#--------------------------------------------------------------------
logAudit(){
	log "$1" "AUDIT"
}

#--------------------------------------------------------------------
# Prints a debug message
# Parameter: Error message
# Returns: N/A
#--------------------------------------------------------------------
logDebug(){
	log "$1" "DEBUG"
}

#--------------------------------------------------------------------
# Performs cleanup task
# Parameter: N/A
# Returns: N/A
#---------------------------------------------------------------------
cleanup(){
	log "Process finished successfully, logs can be found at ${LOG_FILE}"
}

#--------------------------------------------------------------------
# Called using trap on SIGINT, SIGQUIT, SIGABRT, SIGALRM, SIGTERM
# Parameter: Error message
# Returns: N/A
#---------------------------------------------------------------------
interrupt(){
	logError "Process got interrupted with exit code $?! Check error logs in ${LOG_FILE}"
	exit 1
}

#--------------------------------------------------------------------
# Displays a loading indicator for background jobs
# Parameter: Subprocess pid
# Returns: N/A
#---------------------------------------------------------------------
loadingIndicator(){
	local pid=$1
  spin='-\|/'

  local i=0
  while kill -0 $pid 2>/dev/null
  do
    i=$(( (i+1) %4 ))
    # shellcheck disable=SC2059
    printf "\r${spin:$i:1}"
    sleep .1
  done
}
