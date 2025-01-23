import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class AsyncConsumer {

    private final static Logger logger = LoggerFactory.getLogger(SyncConsumer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String GROUP_ID = "test-group";

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        while(true) {

            // 컨슈머는 지속적으로 데이터를 가져오기 위해 반복 호출을 해야 한다.
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String, String> record: records) {
                logger.info("{}", record);
            }

            // 비동기 오프셋 커밋
            /*
                브로커로부터 컨슈머 오프셋이 완료되었음을 받기까지 응답을 기다리지 않는다.
                 -> 동기 오프셋 커밋을 사용할 때보다 동일 시간당 데이터 처리량이 많다.
                 poll() 메서드로 반환된 가장 마지막 레코드의 오프셋을 기준으로 커밋,
             */
//            consumer.commitAsync();

            /*
                비동기로 커밋 응답을 받기 때문에 callback 함수를 파라미터로 받아서 결과를 어등ㄹ
             */
           consumer.commitAsync(new OffsetCommitCallback() {
               @Override
               public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception e) {
                   if(e != null) logger.error("commit failed for offsets {}", offsets, e);
                   else logger.info("Commit succeeded");
               }
           });
        }


    }

}

