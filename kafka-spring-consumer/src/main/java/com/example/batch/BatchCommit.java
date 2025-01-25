package com.example.batch;

import com.example.record.SpringConsumerApplication;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jms.JmsProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;


/*
    spring:
      kafka:
        consumer:
          bootstrap-servers: localhost:9092
        listener:
          type: batch
          ack-mode: MANUAL_IMMEDIATE

     배치 커밋 리스너 설정이다.
        컨테이너에서 관리하는 AckMode를 사용하기 위해 Acknowledgement 인스턴스를 파라미터로 받는다는 점이 다르고,
        커밋을 수행하기 위한 한정적인 메서드만 제공한다.
 */
@SpringBootApplication
public class BatchCommit {

    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);


    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(BatchCommit.class);
        application.run(args);
    }


    @KafkaListener(topics = "test",
            groupId = "test-group-01")
    public void batchListener(ConsumerRecords<String, String> records, Acknowledgment ack) {
        records.forEach(record -> logger.info(record.toString()));
        ack.acknowledge();
    }

    /*
        동기 커밋, 비동기 커밋을 사용하고 싶다면 컨슈머 인스턴스를 파라미터로 받아서 사용할 수 있다. ( commitSync(), commitAsync())
         -> 사용자가 원하는 타이밍에 커밋할 수 있도록 로직을 추가할 수 있다.
         이 경우, 리스너가 커밋하지 않도록 AckMode는 MANUAL 또는 MANUAL_IMMEDIATE로 설정해야 한다.
     */
    @KafkaListener(topics = "test",
            groupId = "test-group-02")
    public void batchListener(List<String> records, Consumer<String, String> consumer) {
        records.forEach(record -> logger.info(record));
        consumer.commitSync();
    }

    @KafkaListener(topics = "test",
            groupId = "test-group-03",
            concurrency = "3")
    public void concurrentBatchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> {logger.info(record.toString());});
    }

}
