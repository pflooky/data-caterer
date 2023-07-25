#!/bin/bash

kafka-topics --create --topic account-topic --bootstrap-server localhost:9092
kafka-topics --describe --topic account-topic --bootstrap-server localhost:9092
kafka-console-consumer --topic account-topic --from-beginning --bootstrap-server localhost:9092
kafka-topics --delete --topic account-topic --bootstrap-server localhost:9092

