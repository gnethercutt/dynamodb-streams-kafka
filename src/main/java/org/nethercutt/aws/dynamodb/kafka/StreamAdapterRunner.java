package org.nethercutt.aws.dynamodb.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public class StreamAdapterRunner {
    private Properties props;
    private static String localddbEndpoint = "http://localhost:8000";

    public StreamAdapterRunner() {
        props = new Properties();
    }
    
    public void run(String[] args) throws FileNotFoundException, IOException, ParseException, InterruptedException {
        Options options = new Options();

        options.addOption("f", true, "properties filename from the filesystem");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        
        if (!cmd.hasOption('f')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("StreamAdapterRunner", options);
            return;
        }
        
        String path = cmd.getOptionValue('f');
        File file = new File(path);
        
        try (FileInputStream fStream = new FileInputStream(file)) {
            props.load(fStream);
        }
        
        KafkaDynamoStreamAdapter streamAdapter = new KafkaDynamoStreamAdapter(
                props.getProperty("dynamodb.sourceTable"), 
                new KafkaForwardingStreamsRecordProcessorFactory(props.getProperty("kafka.brokers"), props.getProperty("kafka.targetTopic")));
        streamAdapter.setClientEndpoints(localddbEndpoint);
        streamAdapter.run();
    }
    
    public static void main(String[] args) throws FileNotFoundException, IOException, ParseException, InterruptedException {
        new StreamAdapterRunner().run(args);
    }

}
