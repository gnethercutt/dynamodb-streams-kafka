package org.nethercutt.aws.dynamodb.kafka;

import java.nio.charset.Charset;

import com.amazonaws.services.kinesis.model.Record;

import kafka.producer.KeyedMessage;

public class MessageFactoryImpl implements MessageFactory {

    @Override
    public KeyedMessage<String, String> createMessage(Record record, String topic) {
        String data = new String(record.getData().array(), Charset.forName("UTF-8"));
        return new KeyedMessage<String, String>(topic, record.getPartitionKey(), data);
    }
}
