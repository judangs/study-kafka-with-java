package stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class SimpleStream {

    private static final String APPLICATION_NAME = "streams-application";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String STREAM_LOG = "stream_log";
    private static final String STREAM_LOG_COPY = "stream_log_copy";

    public static void main(String[] args) {

        Properties props = new Properties();

        /*
            애플리케이션 아이디 값을 기준으로 병렬처리하므로 애플리케이션 아이디를 지정. 또 다른 스트림즈 애플리케이션을 운영한다면
            기존에 작성되지 않았던 이름의 애플리케이션 아이디를 사용해야 한다.
         */
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // 스트림 처리를 위해 메시지 키와 값의 역직렬화, 직렬화 방식을 선택
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        /*
            StreamsBuilder: 스트림 토폴로지를 정의하기 위한 용도로 사용된다.
                소스 프로세서: 최초의 토픽 데이터를 가져온다.
                    - stream() : StreamDSL - KStream
                    - table() : StreamDSL - KTable
                    - globalTable() : StreamDSL - GlobalKTable
               싱크 프로세서: 객체를 다른 토픽으로 전송
                    - to(): KStream 객체를 다른 토픽으로 전송할 수 있다.

         */
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG);
        streamLog.to(STREAM_LOG_COPY);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }

}
