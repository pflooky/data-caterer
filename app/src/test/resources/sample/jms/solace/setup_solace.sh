#!/usr/bin/env bash

echo "Creating new queue in Solace"
curl http://localhost:8080/SEMP/v2/config/msgVpns/default/queues \
  -X POST \
  -u admin:admin \
  -H "Content-type:application/json" \
  -d '{ "queueName":"generated_test_queue" }'

echo "Creating JNDI queue object"
curl http://localhost:8080/SEMP/v2/config/msgVpns/default/jndiQueues \
  -X POST \
  -u admin:admin \
  -H "Content-type:application/json" \
  -d '{ "physicalName":"generated_test_queue", "queueName":"/JNDI/Q/generated_test_queue" }'
