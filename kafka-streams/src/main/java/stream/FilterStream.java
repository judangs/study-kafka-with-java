package stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FilterStream {

    private static final String APPLICATION_NAME = "streams-filter-application";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String STREAM_LOG = "stream_log";
    private static final String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);

        // 데이터를 필터링하는 filter() 메서드는 자바의 함수형 인터페이스인 Predicate를 파라미터로 받는다.
        // Predicate는 함수형 인터페이스로 특정 조건을 표현할 때 사용할 수 있는데 여기서는 메시지 키와 메시지 값에 대한 조건을 나타낸다.
        KStream<String, String> filteredStream = streamLog.filter((key, value) -> value.length() > 5);
        filteredStream.to(STREAM_LOG_FILTER);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }
}
