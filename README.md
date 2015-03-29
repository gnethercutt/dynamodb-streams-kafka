dynamodb-streams-kafka
=======

An implementation of an adapter for publishing DynamoDB Streams notifications to a Kafka topic.

# Getting Started
---------------

* [Zookeeper](https://zookeeper.apache.org/)
* [Kafka](https://kafka.apache.org/)
* The [preview AWS SDK with DynamoDB Streams support](http://dynamodb-preview.s3-website-us-west-2.amazonaws.com/aws-java-sdk-latest-preview.zip)
* [DynamoDB Streams adapter](https://github.com/awslabs/dynamodb-streams-kinesis-adapter)
* (Optional) [DynamoDB Local](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html)

# Building from source
---------------
Install the AWS SDK preview with DynamoDB Streams support:
`cd ~/src/aws-java-sdk-1.9.4a-preview; mvn install:install-file  -Dfile=aws-java-sdk-1.9.4a-preview/lib/aws-java-sdk-1.9.4a-preview.jar  -DgroupId=com.amazonaws  -DartifactId=aws-java-sdk -Dversion=1.9.4a-preview -Dpackaging=jar`
Install the DynamoDB Streams Kinesis adapter:
`cd ~/src/dynamodb-streams-kinesis-adapter; mvn clean install -Dgpg.skip=true`

# Demo
---------------
## Setup the prerequisites

Start zookeeper:        `zkServer start`
Start Kafka:            `kafka-server-start.sh path/to/config/server.properties`
Create topic:           `kafka-topics.sh --create --topic dynamostream --replication-factor 1 --zookeeper localhost:2181 --partitions 8`
Run console listener:   `kafka-console-consumer.sh --zookeeper localhost:2181 --topic dynamostream`
Start DynamoDBLocal:    `java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -inMemory`
Create dynamo table:    `java -cp target/dynamodb-streams-kafka-0.1-SNAPSHOT.jar org.nethercutt.aws.dynamodb.kafka.StreamAdapterDemoHelper -c`

## Run the adapter and generate some DynamoDB traffic
Start the adapter:      `java -jar target/dynamodb-streams-kafka-0.1-SNAPSHOT.jar -f src/main/resources/example.conf
Generate DynamoDB ops:  `java -cp target/dynamodb-streams-kafka-0.1-SNAPSHOT.jar org.nethercutt.aws.dynamodb.kafka.StreamAdapterDemoHelper -t`

## Example Kafka events:
```javascript
{"EventID":"dcc83f06-435f-4d57-b8f9-ba9c27af65ad","EventName":"INSERT","EventVersion":"1.0","EventSource":"aws:dynamodb","AwsRegion":"ddblocal","Dynamodb":{"Keys":{"Id":{"N":"101"}},"NewImage":{"attribute-1":{"S":"test1"},"Id":{"N":"101"}},"SequenceNumber":"000000000000000000005","SizeBytes":26,"StreamViewType":"NEW_IMAGE"}}
{"EventID":"bf688977-6919-4b06-bef0-798fb0abdd87","EventName":"MODIFY","EventVersion":"1.0","EventSource":"aws:dynamodb","AwsRegion":"ddblocal","Dynamodb":{"Keys":{"Id":{"N":"101"}},"NewImage":{"attribute-2":{"S":"test2"},"attribute-1":{"S":"test1"},"Id":{"N":"101"}},"SequenceNumber":"000000000000000000006","SizeBytes":42,"StreamViewType":"NEW_IMAGE"}}
{"EventID":"a47755e5-5b40-418f-a11c-a7c12729fe2b","EventName":"REMOVE","EventVersion":"1.0","EventSource":"aws:dynamodb","AwsRegion":"ddblocal","Dynamodb":{"Keys":{"Id":{"N":"101"}},"SequenceNumber":"000000000000000000007","SizeBytes":5,"StreamViewType":"NEW_IMAGE"}}
{"EventID":"69f864c4-5820-4499-83f1-e9b02ab34ab3","EventName":"INSERT","EventVersion":"1.0","EventSource":"aws:dynamodb","AwsRegion":"ddblocal","Dynamodb":{"Keys":{"Id":{"N":"102"}},"NewImage":{"attribute-1":{"S":"demo3"},"Id":{"N":"102"}},"SequenceNumber":"000000000000000000008","SizeBytes":26,"StreamViewType":"NEW_IMAGE"}}
{"EventID":"2429d052-01c7-4cb0-8594-f4ae30440d4a","EventName":"MODIFY","EventVersion":"1.0","EventSource":"aws:dynamodb","AwsRegion":"ddblocal","Dynamodb":{"Keys":{"Id":{"N":"102"}},"NewImage":{"attribute-2":{"S":"demo4"},"attribute-1":{"S":"demo3"},"Id":{"N":"102"}},"SequenceNumber":"000000000000000000009","SizeBytes":42,"StreamViewType":"NEW_IMAGE"}}
{"EventID":"94d7287f-9107-40f3-885d-5464262f178c","EventName":"REMOVE","EventVersion":"1.0","EventSource":"aws:dynamodb","AwsRegion":"ddblocal","Dynamodb":{"Keys":{"Id":{"N":"102"}},"SequenceNumber":"000000000000000000010","SizeBytes":5,"StreamViewType":"NEW_IMAGE"}}
{"EventID":"e2a70cb4-37c7-47f3-ae97-271d8071357c","EventName":"INSERT","EventVersion":"1.0","EventSource":"aws:dynamodb","AwsRegion":"ddblocal","Dynamodb":{"Keys":{"Id":{"N":"103"}},"NewImage":{"attribute-1":{"S":"demo5"},"Id":{"N":"103"}},"SequenceNumber":"000000000000000000011","SizeBytes":26,"StreamViewType":"NEW_IMAGE"}}
```