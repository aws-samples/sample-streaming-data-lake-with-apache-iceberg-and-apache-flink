#!/bin/bash
# Run any Flink sample locally with the required Java 17 JVM flags.
#
# Usage:
#   ./run-sample.sh <module-name> [extra-java-args...]
#
# Examples:
#   ./run-sample.sh datastream-sample
#   ./run-sample.sh flink-sql-sample
#   ./run-sample.sh dynamic-sink-sample
#   ./run-sample.sh data-generator iceberg-events us-east-1 100 60 v1

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE="$1"

if [ -z "$MODULE" ]; then
  echo "Usage: ./run-sample.sh <module-name> [args...]"
  echo ""
  echo "Available modules:"
  for jar in "$SCRIPT_DIR"/*/target/*-1.0-SNAPSHOT.jar; do
    [ -f "$jar" ] && echo "  $(basename "$(dirname "$(dirname "$jar")")")"
  done
  exit 1
fi

shift

JAR="$SCRIPT_DIR/$MODULE/target/$MODULE-1.0-SNAPSHOT.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found at $JAR"
  echo "Building $MODULE..."
  mvn -f "$SCRIPT_DIR/pom.xml" package -pl "$MODULE" -am -DskipTests -q
fi

# Load Flink Java 17 JVM flags
source "$SCRIPT_DIR/jvm-opts-java17.sh"

echo "Running $MODULE with Java 17 JVM flags..."
exec java $FLINK_JVM_OPTS -jar "$JAR" "$@"
