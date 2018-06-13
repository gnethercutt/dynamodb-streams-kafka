package org.nethercutt.aws.dynamodb.kafka;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class KafkaForwardingStreamsRecordProcessor implements IRecordProcessor {
    private static final int DEFAULT_CHECKPOINT_FREQUENCY = 10;

    private int checkpointCounter;
    private int checkpointFrequency;
    private Producer<String, String> producer;
    private String topic;

    private MessageFactory messageFactory;

    public KafkaForwardingStreamsRecordProcessor(Properties producerConfigProps, String topic) {
        ProducerConfig config = new ProducerConfig(producerConfigProps);
        this.producer = new Producer<String, String>(config);
        this.topic = topic;
        this.checkpointFrequency = DEFAULT_CHECKPOINT_FREQUENCY;
        this.messageFactory = new MessageFactoryImpl();
    }

    @Override
    public void initialize(String shardId) {
        checkpointCounter = 0;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record record : records) {
            producer.send(messageFactory.createMessage(record, topic));

            if(++checkpointCounter % checkpointFrequency == 0) {
                try {
                    checkpointer.checkpoint();
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
        if(reason == ShutdownReason.TERMINATE) {
            try {
                checkpointer.checkpoint();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void setCheckpointFrequency(int checkpointFrequency) {
        this.checkpointFrequency = checkpointFrequency;
    }
    
    public void setMessageFactory(MessageFactory messageFactory) {
        this.messageFactory = messageFactory;
    }
}
