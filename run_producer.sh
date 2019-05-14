source ./kafka.conf
TOPIC="$1"

[ -z "$TOPIC" ] && echo "ERROR: Argument <topic> is missing" && exit 1

[ -t 0 ] && OPT_TTY="-t" && echo "Type input for topic $TOPIC:"

docker exec -i $OPT_TTY $CONTAINER /opt/kafka/bin/kafka-console-producer.sh --topic $TOPIC --broker-list $BOOTSTRAP_SERVER
