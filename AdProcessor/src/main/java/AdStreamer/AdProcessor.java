package AdStreamer;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import java.util.Properties;
import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.settings.Settings;
import java.util.List;
import java.util.ArrayList;
import java.net.InetSocketAddress;
import java.net.InetAddress;

/*
 * Flink class for processing Ads
 */
public class AdProcessor {

    public static void main(String[] args) throws Exception {

        // Get an environment for executing the stream.
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the properties needed to connect to Kafka to fetch the events.
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",
                "ec2-52-41-44-180.us-west-2.compute.amazonaws.com:9092,ec2-52-26-101-162.us-west-2.compute.amazonaws.com:9092,ec2-52-37-93-15.us-west-2.compute.amazonaws.com:9092,ec2-52-38-38-54.us-west-2.compute.amazonaws.com:9092");

        // only required for Kafka 0.8
        properties.setProperty("zookeeper.connect",
                "ec2-52-41-44-180.us-west-2.compute.amazonaws.com:2181,ec2-52-26-101-162.us-west-2.compute.amazonaws.com:2181,ec2-52-37-93-15.us-west-2.compute.amazonaws.com:2181,ec2-52-38-38-54.us-west-2.compute.amazonaws.com:2181");
        // Set to some random group id.
        properties.setProperty("group.id", "test");
        // Disabling auto commit.
        properties.setProperty("auto.commit.enable", "false");
        // Read from the earliest available event in Kafka
        properties.setProperty("auto.offset.reset", "earliest");

        // Set kafka as the event source.
        DataStream<String> stream = see.addSource(
                new FlinkKafkaConsumer08<>(
                    "TwitterAds", // Reading from TwitterAds topic in Kafka
                    new SimpleStringSchema(), // Since we are either receiving JSON or TSV,
                                              // we'll use a simple string schema.
                    properties
                    )
                );

        //stream.print();


        // the rebelance call is causing a repartitioning of the data so that all machines
        // see the messages (for example in cases when "num kafka partitions" < "num flink operators"
        stream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {

                // Convert the incoming tsv string to JSON
                // Assuming the columns are "title, keywords, bid, ttl, url"
                String[] tsv = value.trim().split("\t");
                Map<String, String> esJson = new HashMap<>();
                String action = tsv[0];
                esJson.put("title", tsv[1]);
                esJson.put("keywords", "[" + String.join(",", tsv[2].trim().replace("\"", "").split(",")) + "]");
                esJson.put("bid", tsv[3]);
                esJson.put("ttl", tsv[4]);
                esJson.put("url", tsv[5]);

                return "Kafka -> Flink says: " + esJson.toString();
            }
        }).print(); // print() will write the contents of the stream to the TaskManager's standard out stream

        // Write the json Ad to elasticsearc.
        writeToElastic(stream);

        // Execute the stream environment
        see.execute();
    }

    /*
     * Function to convert the incoming tsv to JSON
     * and write to elasticsearch for indexing
     */
    public static void writeToElastic(DataStream<String> input) {

        Map<String, String> config = new HashMap<>();

        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", "1");

        // Elasticsearch cluster we are writing to
        config.put("cluster.name", "madhavi-ads-cluster");

        try {
            // Add elasticsearch hosts on startup
            // TODO: Optimize this (initialize only once in the constructor)
            List<InetSocketAddress> transports = new ArrayList<>();
            transports.add(
                    new InetSocketAddress(
                        InetAddress.getByName("52.41.44.180"), 9300
                        )
                    ); // port is 9300 not 9200 for ES TransportClient
            transports.add(
                    new InetSocketAddress(
                        InetAddress.getByName("52.38.38.54"), 9300
                        )
                    ); // port is 9300 not 9200 for ES TransportClient
            transports.add(
                    new InetSocketAddress(
                        InetAddress.getByName("52.37.93.15"), 9300
                        )
                    ); // port is 9300 not 9200 for ES TransportClient
            transports.add(
                    new InetSocketAddress(
                        InetAddress.getByName("52.26.101.162"), 9300
                        )
                    ); // port is 9300 not 9200 for ES TransportClient

            ElasticsearchSinkFunction<String> indexLog = new ElasticsearchSinkFunction<String>() {
                // We need to signal elasticsearch to index the ads
                public IndexRequest createIndexRequest(String element) {

                    // Convert the incoming tsv string to JSON
                    // Assuming the columns are "title, keywords, bid, ttl, url"
                    String[] tsv = element.trim().split("\t");
                    Map<String, String> esJson = new HashMap<>();
                    String action = tsv[0];
                    esJson.put("title", tsv[1]);
                    esJson.put("keywords", "[" + String.join(",", tsv[2].trim().replace("\"", "").split(",")) + "]");
                    esJson.put("bid", tsv[3]);
                    esJson.put("ttl", tsv[4]);
                    esJson.put("url", tsv[5]);

                    // Execute index on elasticsearch.
                    return Requests
                            .indexRequest()
                            .index("twitads") // elasticsearch index name is twitads
                            .type("ad")   // Indexing an ad document
                            .source(esJson);
                }

                @Override
                public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                    indexer.add(createIndexRequest(element));
                }
            };


            // Set elasticsearch as the sink for the events consumed from Kafka
            ElasticsearchSink esSink = new ElasticsearchSink(config, transports, indexLog);
            input.addSink(esSink);

        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
