package org.nethercutt.aws.dynamodb.kafka;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class KafkaDynamoStreamAdapter {
    private static final String KCL_WORKER_NAME = "dynamodb-kafka-worker";
    private static final String KCL_APP_NAME = "dynamodb-streams-kafka";
    private static final int DEFAULT_IDLE_TIME_BETWEEN_READS_MSEC = 100;
    private static final int DEFAULT_MAX_RECORDS_PER_READ = 1;
    
    private KinesisClientLibConfiguration workerConfig;
    private IRecordProcessorFactory recordProcessorFactory;
    private AWSCredentialsProvider credentialsProvider;

    private AmazonDynamoDBStreamsAdapterClient adapterClient;
    private AmazonDynamoDBClient dynamoDBClient;
    private AmazonCloudWatchClient cloudWatchClient;
    private String sourceTable;
    private String streamId;
    private Thread workerThread;
    private Worker worker;
    
    private int idleTimeBetweenReads = DEFAULT_IDLE_TIME_BETWEEN_READS_MSEC;
    private int maxRecordsPerRead = DEFAULT_MAX_RECORDS_PER_READ;

    public KafkaDynamoStreamAdapter(String brokerList, String srcTable, String targetTopic) {
        this(srcTable, new KafkaForwardingStreamsRecordProcessorFactory(brokerList, targetTopic));
    }
    
    public KafkaDynamoStreamAdapter(String srcTable, IRecordProcessorFactory processorFactory) {
        sourceTable = srcTable;
        credentialsProvider = new DefaultAWSCredentialsProviderChain();
        recordProcessorFactory = processorFactory;

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(credentialsProvider, new ClientConfiguration());
        dynamoDBClient = new AmazonDynamoDBClient(credentialsProvider, new ClientConfiguration());
        cloudWatchClient = new AmazonCloudWatchClient(credentialsProvider, new ClientConfiguration());
    }
    
    AmazonDynamoDBClient getDynamoDBClient() {
        return dynamoDBClient;
    }
    
    public void setClientEndpoints(String endpoint) {
        adapterClient.setEndpoint(endpoint);
        dynamoDBClient.setEndpoint(endpoint);
    }
    
    public int getIdleTimeBetweenReads() {
        return idleTimeBetweenReads;
    }
    
    public void setIdleTimeBetweenReads(int msec) {
        idleTimeBetweenReads = msec;
    }
    
    public int getMaxRecordsPerRead() {
        return maxRecordsPerRead;
    }
    
    public void setMaxRecordsPerRead(int maxRecords) {
        maxRecordsPerRead = maxRecords;
    }
    
    private String enableStreamForTable(AmazonDynamoDBClient client, StreamViewType viewType, String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest()
            .withTableName(tableName);
        DescribeTableResult describeResult = client.describeTable(describeTableRequest);
        if (describeResult.getTable().getStreamSpecification().isStreamEnabled()) {
            //TODO: what if the viewtype doesn't match
            return describeResult.getTable().getLatestStreamId();
        }

        StreamSpecification streamSpecification = new StreamSpecification();
        streamSpecification.setStreamEnabled(true);
        streamSpecification.setStreamViewType(viewType);
        UpdateTableRequest updateTableRequest = new UpdateTableRequest()
            .withTableName(tableName)
            .withStreamSpecification(streamSpecification);

        UpdateTableResult result = client.updateTable(updateTableRequest);
        return result.getTableDescription().getLatestStreamId();
    }
    
    public void run() {
        streamId = enableStreamForTable(dynamoDBClient, StreamViewType.NEW_IMAGE, sourceTable);
        workerConfig = new KinesisClientLibConfiguration(KCL_APP_NAME, streamId, credentialsProvider, KCL_WORKER_NAME)
            .withMaxRecords(maxRecordsPerRead)
            .withIdleTimeBetweenReadsInMillis(idleTimeBetweenReads)
            .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);   
        worker = new Worker(recordProcessorFactory, workerConfig, adapterClient, dynamoDBClient, cloudWatchClient);
        workerThread = new Thread(worker);
        workerThread.start();
    }
    
    public void shutdown() throws InterruptedException {
        worker.shutdown();
        workerThread.join();  
    }
}
