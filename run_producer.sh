
# Password and api keys
. ./.pws

# setup environment variables
. ./.exps

go run -v cmd/producer.go
#./bin/producer

# https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html
# kafkacat -b localhost:9092 -t SNDBX_AppLab
