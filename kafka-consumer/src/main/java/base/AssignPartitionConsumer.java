package base;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/*
    컨슈머를 운영할 때에
        - subscribe(): 컨슈머를 운영할 때 구독 형태로 운영
        - assign(): 컨슈머를 운영할 때 파티션을 컨슈머에 명시적으로 할당하여 운영
 */
public class AssignPartitionConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SyncConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String GROUP_ID = "test-group";
    private final static int PARTITION_NUMBER = 0;

    private static KafkaConsumer<String, String> consumer;

    /*
        셧다운 훅(Shutdown hook): 사용자 또는 운영체제로부터 종료 요청을 받으면 실행하는 스레드
     */
    static class ShutdonwThread extends Thread {
        public void run() {
            logger.info("Shutdown hook");
            consumer.wakeup();
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
        // 컨슈머를 운영할 때 파티션을 컨슈머에 명시적으로 할당
        consumer.assign(Collections.singleton(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));
        // 컨슈머에 할당된 토픽과 파티션에 대한 정보
//        Set<TopicPartition> assginedTopicPArtition = consumer.assignment();

        /*
            정상적으로 종료되지 않은 컨슈머는 세션 타임아웃이 발생할때까지 컨슈머 그룹에 남게 된다.
            이로 인해 실제로 종료되었지만 더는 동작을 하지 않는 컨슈머가 존재하기 때문에 파티션의 데이터는 소모되지 못하고 컨슈머 랙이 증가
                ※ 컨슈머 랙(LAG): 토픽의 최신 오프셋과 컨슈머 오프셋 간의 차이
         */
        try {
            while(true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord<String, String> record: records) {
                    logger.info("{}", record);
                }

                consumer.commitSync();
                Runtime.getRuntime().addShutdownHook(new ShutdonwThread());
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup consumer");
        } finally {

            // 해당 컨슈머는 더는 동작하지 않는다는 것을 명시적으로 알려주므로 컨슈머 그룹에서 이탈되고 나머지 컨슈머들이 파티션을 할당받게 된다.
            consumer.close();
        }
    }
}
