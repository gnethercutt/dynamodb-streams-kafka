package org.nethercutt.aws.dynamodb.kafka;

import kafka.producer.KeyedMessage;

import com.amazonaws.services.kinesis.model.Record;

public interface MessageFactory {

    KeyedMessage<String, String> createMessage(Record record, String topic);

}
