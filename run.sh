#!/bin/bash

if [ -z "$1" ]; then
  echo "Usage: $0 <dependency-jar-directory>"
  exit 1
fi

DEP_DIR="$1"

if [ ! -d "$DEP_DIR" ]; then
  echo "Error: Directory '$DEP_DIR' does not exist."
  exit 1
fi

FLAGS=$(for jar in "$DEP_DIR"/*.jar; do
  [ -e "$jar" ] || continue
  echo -n "-C file://$jar "
done)

/opt/flink/bin/flink run -c StreamingJob $FLAGS span.jar config.properties
