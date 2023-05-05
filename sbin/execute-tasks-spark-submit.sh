#!/usr/bin/env bash

# Read initial variables
HOME_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${HOME_DIRECTORY}" || exit # exit in case cd fails; very rare
export PYTHONPATH=${PYTHONPATH}:"${HOME_DIRECTORY}/../src/"
export VIRTUALENVWRAPPER_PYTHON="$(which python3)"
export WORKON_HOME="$HOME/python_venvs/"
export PROJECT_HOME="${HOME_DIRECTORY}../"
source virtualenvwrapper.sh
source "$HOME/python_venvs/hello-fresh-data-engg/bin/activate"

while getopts ":e:c:m:" arg; do
	case "${arg}" in
    e)
      NUM_EXECS=${OPTARG}
      ;;
    c)
      EXEC_CORES=${OPTARG}
      ;;
    m)
      EXEC_MEM=${OPTARG}
      ;;
		*)
			scriptUsage
			;;
	esac
done
shift $((OPTIND-1))

if [[ -z ${NUM_EXECS} || -z ${EXEC_CORES} || -z ${EXEC_MEM} ]]; then
  NUM_EXECS="2"
  NUM_CORES="1"
  EXEC_MEM="1g"
fi

FILES="${HOME_DIRECTORY}/../conf/data-quality/rules/production_configs/recipe-task1-dq-rules.json,${HOME_DIRECTORY}/../conf/data-quality/rules/production_configs/recipe-task2-dq-rules.json,${HOME_DIRECTORY}/../conf/spark/log4j.properties"

spark-submit \
--master local[*]	\
--name "HelloFresh Data Engineering Recipe tasks" \
--driver-memory 1g \
--num-executors "${NUM_EXECS}" \
--executor-cores "${NUM_CORES}" \
--executor-memory "${EXEC_MEM}" \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.yarn.maxAppAttempts=1 \
--conf spark.serializer="org.apache.spark.serializer.KryoSerializer" \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j.properties" \
--files "${FILES}" \
../src/com/vitthalmirji/datapipelines/recipe_tasks.py --input-data-dir "${HOME_DIRECTORY}/../resources/data/input" --output-data-dir "${HOME_DIRECTORY}/../resources/data/output"
