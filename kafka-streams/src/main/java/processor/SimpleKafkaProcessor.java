package processor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class SimpleKafkaProcessor {

    private static String APPLICATION_NAME = "processor-application";
    private static String BOOTSTRAP_SERVER = "localhost:9092";
    private static String STREAM_LOG = "stream_log";
    private static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /*
            토폴로지 구성을 위해 소스 / 스트림 / 싱크 프로세서를 설정해야 한다.
         */
        Topology topology = new Topology();
        topology.addSource("Source", STREAM_LOG)
                .addProcessor("Process", () -> new FilterProcessor(),
                        "Source")
                .addSink("Sink",
                        STREAM_LOG_FILTER,
                        "Process");

        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();
    }
}
