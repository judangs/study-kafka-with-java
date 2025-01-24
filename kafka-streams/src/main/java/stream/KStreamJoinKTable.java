package stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class KStreamJoinKTable {

    private static final String APPLICATION_NAME = "order-join-application";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final String ADDRESS_TABLE = "address";
    private static final String ORDER_STREAM = "order";
    private static final String ORDER_JOIN_STREAM = "order_join";


    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // StreamsBuilder 생성
        StreamsBuilder builder = new StreamsBuilder();

        // KTable 생성 (address 테이블을 읽어오기)
        KTable<String, String> addressTable = builder.table(ADDRESS_TABLE);

        // KStream 생성 (order 스트림을 읽어오기)
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        // KStream과 KTable을 join
        orderStream.join(
                addressTable, // 조인할 KTable
                (order, address) -> order + " send to " + address // 조인 후 결과 생성하는 람다
        ).to(ORDER_JOIN_STREAM); // 조인 결과를 output stream에 전송

        // Kafka Streams 인스턴스 생성 및 시작
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
