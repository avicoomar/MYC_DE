# Local env variables, different for every systems, make sure to adjust accordingly. Use this as a template if needed

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export SPARK_HOME="$SCRIPT_DIR/spark-4.0.0-bin-hadoop3"

PY4J_ZIP=$(echo ${SPARK_HOME}/python/lib/py4j-*.zip)
PYSPARK=${SPARK_HOME}/python
export PYTHONPATH="${PYSPARK}:${PY4J_ZIP}${PYTHONPATH:+:$PYTHONPATH}"
