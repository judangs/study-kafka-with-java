package com.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@SpringBootApplication
public class SpringConsumerApplication implements CommandLineRunner {

    public static final Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);

    /*
        가장 기본적인 리스너 선언으로 poll()이 호출되어 가져온 레코드들은 차례대로 개별 레코드의 메시지 값을 파라미터로 받게 된다.
     */
    @KafkaListener(topics = "test",
        groupId = "test-group-00")
    public void recordListener(ConsumerRecord<String, String> record) {
        logger.info(record.toString());
    }

    /*
        스프링 카프카의 역직렬화 클래스 기본값인 StringDeserializer를 사용했으므로 String 클래스로 메시지 값을 전달받을 수 있다.
     */
    @KafkaListener(topics = "test",
            groupId = "test-group-01")
    public void singleTopicListener(String message) {
        logger.info(message);
    }

    @KafkaListener(topics = "test",
            groupId = "test-group-02",
            properties = {
                "max.poll.interval.ms:60000",
                "auto.offset.reset:earliest"
            })
    public void singleTopicWithPropertiesListener(String message) {
        logger.info(message);
    }

    /*
        2개 이상의 카프카 컨슈머 스레드를 실행하고 싶다면 concurrency 옵션을 사용하면 된다.
        - concurrency: 해당하는 만큼 컨슈머 스레드를 만들어서 병렬처리한다.
     */
    @KafkaListener(topics = "test",
            groupId = "test-group-03",
            concurrency = "3")
    public void concurrentTopicListener(String message) {
        logger.info(message);
    }

    /*
        특정 토픽의 특정 파티션만 구독하고 싶은 경우.
        추가로 특정 파티션의 특정 오프셋까지 지정할 수 있다. 이 경우 그룹 아이디에 관계없이 항상 설정한 오프셋의 데이터부터 가져온다.
     */
    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "test01", partitions = {"0", "1"}),
            @TopicPartition(topic = "test03", partitionOffsets =
                @PartitionOffset(partition = "0", initialOffset = "3"))
    }, groupId = "test-group-04")
    public void listenSpecificPartition(ConsumerRecord<String, String> record) {
        logger.info(record.toString());
    }

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) throws Exception {

    }
}
