package org.nethercutt.aws.dynamodb.kafka;

import java.util.Properties;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class KafkaForwardingStreamsRecordProcessorFactory implements IRecordProcessorFactory {
    private String topic;
    private Properties props;

    public KafkaForwardingStreamsRecordProcessorFactory(String brokerList, String topic) { 
        this.topic = topic;
        this.props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
    }

    @Override
    public IRecordProcessor createProcessor() {
        return new KafkaForwardingStreamsRecordProcessor(props, topic);
    }

}
