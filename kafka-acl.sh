#!/bin/bash

# Variables
KAFKA_BIN="/usr/local/kafka-server/bin"
# KAFKA_BIN="/home/adkhamsec/Documents/projects/kafka-server/bin"
KAFKA_URL="https://dlcdn.apache.org/kafka/4.0.0/kafka_2.13-4.0.0.tgz"
KAFKA_DIR="usr/local/kafka-server"
# KAFKA_DIR="/home/adkhamsec/Documents/projects/kafka-server"
KAFKA_TAR="kafka_2.13-4.0.0.tgz"

KAFKA_USERS_FILE="./kafka-users.txt"


# Function to check if the given IP address and port is reachable with telnet
check_broker_accessibility() {
    local BROKER_IP="$1"
    local BROKER_PORT="$2"

    echo "Checking if Kafka broker at $BROKER_IP:$BROKER_PORT is reachable..."
    if nc -zv "$BROKER_IP" "$BROKER_PORT" 2>&1 | grep -q succeeded; then
        echo "Broker $BROKER_IP:$BROKER_PORT is reachable."
    else
        echo "Broker $BROKER_IP:$BROKER_PORT is not reachable. Please check the IP and port."
        exit 1
    fi
}

# Function to check if a user is in the file
check_user() {
    local input_user="$1"
    grep -q "^[[:space:]]*user_${input_user}=" "$KAFKA_USERS_FILE"
}
# Function to append the user to the KafkaServer section

append_user() {
    local input_user="$1"
    local input_password="$2"

    echo "Appending user '$input_user' to the KafkaServer section..."

    awk -v new_user="$input_user" -v new_pass="$input_password" '
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
            # Remove semicolon from the last user line
            if (i == last_user_line) {
                sub(/;[[:space:]]*$/, "", lines[i])
            }
            # Before the closing brace of KafkaServer, insert the new user
            if (i == kafka_end_line) {
                printf "   user_%s=\"%s\";\n", new_user, new_pass
            }
            print lines[i]
        }
    }
    ' "$KAFKA_USERS_FILE" > "${KAFKA_USERS_FILE}.tmp" && mv "${KAFKA_USERS_FILE}.tmp" "$KAFKA_USERS_FILE"

    echo "User '$input_user' added to KafkaServer section with password '$input_password'."
}


# Function to generate an 8-character alphanumeric password
generate_password() {
    tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 8
}


# Function to install Kafka CLI tools
install_kafka() {
    # Create kafaka Directory
    mkdir -p "$KAFKA_DIR"

    # Download Kafka tar file
    echo "Downloading Kafka from $KAFKA_URL..."
    wget "$KAFKA_URL" -O "$KAFKA_TAR"

    tar -xzf "$KAFKA_TAR" -C "$KAFKA_DIR" --strip-components=1

    # Clean up
    rm "$KAFKA_TAR"

    echo "Kafka CLI tools installed in $KAFKA_DIR."
}

# Function to check if Kafka CLI tools are installed in the specified directory
check_kafka_cli_tools() {
    if [ ! -f "$KAFKA_BIN/kafka-topics.sh" ] || [ ! -f "$KAFKA_BIN/kafka-acls.sh" ]; then
        echo "Kafka CLI tools not found in $KAFKA_BIN."
        read -p "Do you want to download Kafka? (y/n): " INSTALL_KAFKA
        if [[ "$INSTALL_KAFKA" =~ ^[Yy]$ ]]; then
            echo "Installing Kafka CLI tools..."
            install_kafka
        else
            echo "Exiting script as Kafka CLI tools are required."
            exit 1
        fi
    else
        echo "Kafka CLI tools found in $KAFKA_BIN."
    fi
}


# Broker IP and port

read -p "Please geve the IP address of the broker " BROKER_IP
read -p "Please give PORT number of the broker " BROKER_PORT

# Check if the Kafka broker is reachable
check_broker_accessibility "$BROKER_IP" "$BROKER_PORT"

# Check if Kafka CLI tools are installed
check_kafka_cli_tools


# Ask for a username and validate it
read -p "For what user should I create the topic? " USERNAME

if ! check_user "$USERNAME"; then
    echo "User '$USERNAME' not found in KafkaServer section. Creating user automatically..."
    PASSWORD=$(generate_password)
    # Append the new user to KafkaServer section
    append_user "$USERNAME" "$PASSWORD"
fi

read -p "What topic should I create? " TOPIC_NAME

read -p "How many partitions should I create for this topic? " PARTITIONS

read -p "What should be the replication factor for this topic? " REPLLICATION

echo "What role to produce?"
echo "1. Producer"
echo "2. Consumer"
read -p "Please select a role (1 or 2): " ROLE_CHOICE

case $ROLE_CHOICE in
    1)
        ROLE="Producer"
        PERMISSIONS="--operation Read --operation Write --operation Describe"
        ;;
    2)
        ROLE="Consumer"
        PERMISSIONS="--operation Read --operation Describe"
        ;;
    *)
        echo "Invalid choice. Exiting."
        exit 1
        ;;
esac


# Create the topic using Kafka CLI
echo "Creating Kafka topic '$TOPIC_NAME' with $PARTITIONS partitions and replication factor $REPLLICATION..."

$KAFKA_BIN/kafka-topics.sh --create \
    --topic "$TOPIC_NAME" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLLICATION" \
    --bootstrap-server "$BROKER_IP:$BROKER_PORT"

$KAFKA_BIN/kafka-acls.sh --bootstrap-server "$BROK  ER_IP:$BROKER_PORT" \
  --add \
  --allow-principal "User:$USERNAME" \
  --topic "$TOPIC_NAME" \
  $PERMISSIONS
