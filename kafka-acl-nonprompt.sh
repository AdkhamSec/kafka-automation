#!/bin/bash

# Default values
BROKER=""
PORT=""
TOPIC=""
PARTITION=""
REPLICATION_FACTOR=""
PRODUCER=""
CONSUMER=""

KAFKA_BIN="/usr/local/kafka-server/bin"
KAFKA_URL="https://dlcdn.apache.org/kafka/4.0.0/kafka_2.13-4.0.0.tgz"
KAFKA_DIR="/usr/local/kafka-server"
KAFKA_TAR="kafka_2.13-4.0.0.tgz"

KAFKA_USERS_FILE="./kafka-users.txt"
KAFKA_SECRETS_FILE="./kafka-secrets.txt"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      echo "Usage: $0 [OPTIONS]
  -b     Broker IP address and port (e.g. 192.168.1.100:9092)
  -prod  Producer username
  -cons  Consumer username
  -t     Topic name
  -p     Number of partitions
  -rf    Replication factor
  -h     Show this help message"
      exit 0
      ;;
    -b)
      BROKER="$2"
      BROKER_IP=$(echo "$BROKER" | cut -d':' -f1)
      BROKER_PORT=$(echo "$BROKER" | cut -d':' -f2)
      shift 2
      ;;
    -prod)
      PRODUCER="$2"
      shift 2
      ;;
    -cons)
      CONSUMER="$2"
      shift 2
      ;;
    -t)
      TOPIC="$2"
      shift 2
      ;;
    -p)
      PARTITION="$2"
      shift 2
      ;;
    -rf)
      REPLICATION_FACTOR="$2"
      shift 2
      ;;
    -*)
      echo "Unknown option: $1"
      "$0" -h
      exit 1
      ;;
    *)
      echo "Unexpected argument: $1"
      "$0" -h
      exit 1
      ;;
  esac
done

# --- FUNCTIONS ---

check_broker_accessibility() {
    local IP="$1"
    local PORT="$2"
    echo "Checking if Kafka broker at $IP:$PORT is reachable..."
    if nc -zv "$IP" "$PORT" 2>&1 | grep -q succeeded; then
        echo "Broker $IP:$PORT is reachable."
    else
        echo "Broker $IP:$PORT is not reachable. Please check the IP and port."
        exit 1
    fi
}

check_user() {
    local user="$1"
    grep -q "^[[:space:]]*user_${user}=" "$KAFKA_USERS_FILE"
}

append_user() {
    local user="$1"
    local password="$2"
    echo "Appending user '$user' to the KafkaServer section..."

    awk -v new_user="$user" -v new_pass="$password" '
    BEGIN {
        in_server = 0
        last_user_line = 0
    }
    {
        lines[NR] = $0
        if ($0 ~ /KafkaServer[[:space:]]*{/) {
            in_server = 1
        } else if (in_server && $0 ~ /^[[:space:]]*}/) {
            in_server = 0
            kafka_end_line = NR
        } else if (in_server && $0 ~ /^[[:space:]]*user_[^=]+="[^"]*";[[:space:]]*$/) {
            last_user_line = NR
        }
    }
    END {
        for (i = 1; i <= NR; i++) {
            if (i == last_user_line) {
                sub(/;[[:space:]]*$/, "", lines[i])
            }
            if (i == kafka_end_line) {
                printf "   user_%s=\"%s\";\n", new_user, new_pass
            }
            print lines[i]
        }
    }
    ' "$KAFKA_USERS_FILE" > "${KAFKA_USERS_FILE}.tmp" && mv "${KAFKA_USERS_FILE}.tmp" "$KAFKA_USERS_FILE"

    echo "User '$user' added to KafkaServer section with password '$password'."
}

generate_password() {
    tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 8
}

install_kafka() {
    mkdir -p "$KAFKA_DIR"
    echo "Downloading Kafka from $KAFKA_URL..."
    wget "$KAFKA_URL" -O "$KAFKA_TAR"
    tar -xzf "$KAFKA_TAR" -C "$KAFKA_DIR" --strip-components=1
    rm "$KAFKA_TAR"
    echo "Kafka CLI tools installed in $KAFKA_DIR."
}

check_kafka_cli_tools() {
    if [ ! -f "$KAFKA_BIN/kafka-topics.sh" ] || [ ! -f "$KAFKA_BIN/kafka-acls.sh" ]; then
        echo "Kafka CLI tools not found in $KAFKA_BIN."
        echo "Exiting script as Kafka CLI tools are required."
        exit 1
    else
        echo "Kafka CLI tools found in $KAFKA_BIN."
    fi
}

is_topic_exists() {
    local topic_name="$1"
    $KAFKA_BIN/kafka-topics.sh --list --bootstrap-server "$BROKER_IP:$BROKER_PORT" --command-config "$KAFKA_SECRETS_FILE" | grep -q "^$topic_name$"
}

# --- MAIN EXECUTION ---

check_broker_accessibility "$BROKER_IP" "$BROKER_PORT"
check_kafka_cli_tools

# Ensure users exist
if [ -n "$PRODUCER" ]; then
  if ! check_user "$PRODUCER"; then
    echo "Producer user '$PRODUCER' not found. Creating..."
    PASSWORD=$(generate_password)
    append_user "$PRODUCER" "$PASSWORD"
  fi
fi

if [ -n "$CONSUMER" ]; then
  if ! check_user "$CONSUMER"; then
    echo "Consumer user '$CONSUMER' not found. Creating..."
    PASSWORD=$(generate_password)
    append_user "$CONSUMER" "$PASSWORD"
  fi
fi

# Create topic if it doesn't exist
if is_topic_exists "$TOPIC"; then
    echo "Topic '$TOPIC' already exists. Skipping creation."
else
    echo "Creating Kafka topic '$TOPIC' with $PARTITION partitions and replication factor $REPLICATION_FACTOR..."
    $KAFKA_BIN/kafka-topics.sh --create \
        --topic "$TOPIC" \
        --partitions "$PARTITION" \
        --replication-factor "$REPLICATION_FACTOR" \
        --bootstrap-server "$BROKER_IP:$BROKER_PORT" \
        --command-config "$KAFKA_SECRETS_FILE"
fi

# Assign ACLs
if [ -n "$PRODUCER" ]; then
    echo "Applying ACLs for producer: $PRODUCER"
    $KAFKA_BIN/kafka-acls.sh --bootstrap-server "$BROKER_IP:$BROKER_PORT" \
        --add \
        --allow-principal "User:$PRODUCER" \
        --topic "$TOPIC" \
        --operation Read \
        --operation Write \
        --operation Describe \
        --command-config "$KAFKA_SECRETS_FILE"
fi

if [ -n "$CONSUMER" ]; then
    echo "Applying ACLs for consumer: $CONSUMER"
    $KAFKA_BIN/kafka-acls.sh --bootstrap-server "$BROKER_IP:$BROKER_PORT" \
        --add \
        --allow-principal "User:$CONSUMER" \
        --topic "$TOPIC" \
        --operation Read \
        --operation Describe \
        --command-config "$KAFKA_SECRETS_FILE"
fi
