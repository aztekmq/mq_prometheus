./mq_traffic/build.sh 

read -rsp 'MQ password: ' MQ_TRAFFIC_PASSWORD
printf '\n'
MQ_TRAFFIC_PASSWORD="$MQ_TRAFFIC_PASSWORD" ./mq_traffic/run.sh