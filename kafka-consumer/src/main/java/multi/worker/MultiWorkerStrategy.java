package multi.worker;

import base.SyncConsumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
    멀티 워커 스레드 전략
     - 브로커로부터 전달받은 레코드를 병렬로 처리하기 위해 1개의 컨슈머 스레드로 받은 데이터를 멀티 스레드(worker thread)를 이용하여 데이터를 처리하는 전략

 */
public class MultiWorkerStrategy {

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

        /*
            작업 이후 스레드가 종료되어야 한다면 CacheThreadPoll을 사용하여 스레드를 실행할 수 있다.
            CacheThreadPool:
                필요한 만큼 스레드 풀을 늘려서 스레드를 실행하는 방식.
                짧은 생명주기를 가진 스레드에서 유용
         */
        ExecutorService executorService = Executors.newCachedThreadPool();

        while(true) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            for(ConsumerRecord<String, String> record: records) {

                /*
                문제점:
                    - 커밋 설정이 오토 커밋인 경우 스레드에서 데이터 처리가 끝나지 않았음을 리턴받지 않고 오프셋 커밋을 완료하기 때문에 데이터 유실이 발생할 수 있다.
                    - 레코드 역전 현상:
                        for 반복구문으로 스레드를 생성하므로 레코드별로 스레드의 생성은 순서대로 진행되지만, 나중에 생성된 스레드의 레코드 처리 시간이 더 짧을 경우
                        이전 레코드보다 다음 레코드가 먼저 처리될 수 있다. 이로 인해 레코드의 순서가 뒤바뀌는 현상이 발생할 수 있다.
                 따라서 레코드 처리에 있어 중복이 발생하거나 데이터의 역전현상이 발생해도 되지만, 매우 빠른 처리속도가 필요한 데이터 처리에 적합하다.
                 ex) 서버 리소스 모니터링 파이프라인, IoT 서비스의 센서 데이터 수집 파이프라인 등.
                 */
                ConsumerWorker worker = new ConsumerWorker(record.value());
                executorService.execute(worker);
            }
        }

    }
}
