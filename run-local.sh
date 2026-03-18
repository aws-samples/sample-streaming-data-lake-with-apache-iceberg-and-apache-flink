#!/bin/bash
# Run a Flink sample locally with Java 17 --add-opens flags.
# These flags are needed because Iceberg's checkpoint serialization
# uses Kryo which requires reflective access to java.nio internals.
# On AWS Managed Flink, these flags are set automatically.
#
# Usage: ./run-local.sh <module> [args...]
# Examples:
#   ./run-local.sh flink-sql-sample
#   ./run-local.sh datastream-sample
#   ./run-local.sh data-generator iceberg-events us-east-1 100 60 v1

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MODULE="$1"
[ -z "$MODULE" ] && echo "Usage: ./run-local.sh <module> [args...]" && exit 1
shift

JAR="$SCRIPT_DIR/$MODULE/target/$MODULE-1.0-SNAPSHOT.jar"
[ ! -f "$JAR" ] && echo "Building $MODULE..." && mvn -f "$SCRIPT_DIR/pom.xml" package -pl "$MODULE" -am -DskipTests -q

exec java \
  --add-opens java.base/java.nio=ALL-UNNAMED \
  --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens java.base/java.lang=ALL-UNNAMED \
  --add-opens java.base/java.util=ALL-UNNAMED \
  -jar "$JAR" "$@"
