# KAFKA SHOVELING
**Kafka shovel moves error topic to retry topic. To enable shoveling mechanism for any kafka topic we need to modify config.json 
You need to configure brokers, for example, "brokers": "kafka-stg-broker-01.getir.com". 
You need to configure also `topics` part. You need to add desired topic that need to be shoveled by this application. You also need to set `retryCount`.
You may set this field to limit number of shovel if you have set isFinite field true, `retryCount` will be meaningless**