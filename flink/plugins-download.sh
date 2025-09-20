#!/usr/bin/env bash
set -euo pipefail

# Script location (this file lives in the flink/ directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="$SCRIPT_DIR/plugins"

mkdir -p "$PLUGIN_DIR"
cd "$PLUGIN_DIR"

# Versions (adjust if you upgrade Flink)
FLINK_VERSION=1.16.1
PULSAR_RUNTIME_VERSION=3.0.1-1.16
KAFKA_VERSION=1.16.1
JDBC_VERSION=1.16.1
FILESYSTEM_VERSION=1.16.1

# Maven Central URLs (correct artifact names)
SQL_PULSAR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-pulsar/${FLINK_VERSION}/flink-sql-connector-pulsar-${FLINK_VERSION}.jar"
RUNTIME_PULSAR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-connector-pulsar/${PULSAR_RUNTIME_VERSION}/flink-connector-pulsar-${PULSAR_RUNTIME_VERSION}.jar"
KAFKA_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_VERSION}/flink-sql-connector-kafka-${KAFKA_VERSION}.jar"
JDBC_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_VERSION}/flink-connector-jdbc-${JDBC_VERSION}.jar"
FILESYSTEM_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-connector-files/${FILESYSTEM_VERSION}/flink-connector-files-${FILESYSTEM_VERSION}.jar"

download() {
  local url=$1
  local jar_name
  jar_name="$(basename "$url")"
  if [[ -f "$jar_name" ]]; then
    echo "⚡ Already present: $jar_name"
  else
    echo "📥 Downloading $jar_name"
    curl -fSL -o "$jar_name" "$url"
  fi
}

echo "Working in plugin dir: $PLUGIN_DIR"

# Download connectors
download "$SQL_PULSAR_URL"
download "$RUNTIME_PULSAR_URL"
download "$KAFKA_URL"
download "$JDBC_URL"
download "$FILESYSTEM_URL"

echo "✅ Connectors downloaded to $PLUGIN_DIR"
ls -lh "$PLUGIN_DIR"
