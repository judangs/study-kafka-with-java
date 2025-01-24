package stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KStreamJoinGlobalKTable {

    private static String APPLICATION_NAME = "global-table-join-application";
    private static String BOOTSTRAP_SERVERS = "localhost:9092";
    private static String ADDRESS_GLOBAL_TABLE = "address_v2";
    private static String ORDER_STREAM = "order";
    private static String ORDER_JOIN_STREAM = "order_join";

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, String> addressGlobalTable = builder.globalTable(ADDRESS_GLOBAL_TABLE);
        KStream<String, String> orderStream = builder.stream(ORDER_STREAM);

        /*
            GlobalKTable:
                - 토픽에 존재하는 모든 데이터를 태스크마다 저장하고 조인 처리를 수행하는 점.
                - KTable의 조인과 다르게 레코드를 매칭할 때 KStream의 메시지 키와 메시지 값 둘 다 사용할 수 있다.
                ※ M1 칩을 사용하는 Mac에서 RocksDB 아키텍처 호완성 문제 해결
                    링크: https://www.inflearn.com/community/questions/1236771/kstreamjoinktable-%EC%8B%A4%ED%96%89%EC%8B%9C-%EC%97%90%EB%9F%AC?focusComment=332392&srsltid=AfmBOoqoci3JqqNYR0lczAm4Rsh0hfWCqTy6xVXZV1dLpX89y-e1Wyfn

         */
        orderStream.join(addressGlobalTable,
                (orderKey, orderValue) -> orderKey,
                (order, address) -> order + " send to " + address)
                .to(ORDER_JOIN_STREAM);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

    }

}
