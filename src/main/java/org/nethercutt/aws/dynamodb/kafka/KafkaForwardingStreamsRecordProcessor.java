package org.nethercutt.aws.dynamodb.kafka;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

public class KafkaForwardingStreamsRecordProcessor implements IRecordProcessor {
    private static final int DEFAULT_CHECKPOINT_FREQUENCY = 10;

    private int checkpointCounter;
    private int checkpointFrequency;
    private Producer<String, String> producer;
    private String topic;

    public KafkaForwardingStreamsRecordProcessor(Properties producerConfigProps, String topic) {
        ProducerConfig config = new ProducerConfig(producerConfigProps);
        producer = new Producer<String, String>(config);
        this.topic = topic;
        this.checkpointFrequency = DEFAULT_CHECKPOINT_FREQUENCY;
    }

    @Override
    public void initialize(String shardId) {
        checkpointCounter = 0;
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
        for (Record record : records) {
            String data = new String(record.getData().array(), Charset.forName("UTF-8"));
            KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, record.getPartitionKey(), data);
            producer.send(msg);

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
}
