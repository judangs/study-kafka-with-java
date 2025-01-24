package base;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/*
    리밸런스 발생 시 데이터를 중복으로 처리하지 않기 위해서는 처리한 데이터를 기준으로 커밋을 시도해야 한다.
 */
public class RebalanceConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SyncConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String GROUP_ID = "test-group";

    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets;

    private static class RebalanceListner implements ConsumerRebalanceListener {

        // 리밸런스가 시작되기 직전에 호출되는 메서드
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            logger.warn("Partition are revoked");
            consumer.commitSync(currentOffsets);
        }

        // 리밸런스가 끝난 뒤에 파티션이 할당 완료되면 호출되는 메서드
        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
            logger.warn("Partition are assigned");
        }
    }

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListner());

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);

                /*
                    레코드의 데이터 처리가 끝나면 레코드가 속한 토픽, 파티션, 오프셋에 관한 정보를 HashMap에 담아 오프셋 지정 커밋 시 사용
                    offset + 1: 컨슈머 재시작 시에 파티션에서 가장 마지막으로 커밋된 오프셋부터 레코드를 읽기 시작
                 */
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1, null));
                consumer.commitSync(currentOffsets);
            }
        }
    }
}
