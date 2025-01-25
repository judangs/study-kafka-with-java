package com.example.facotry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication(scanBasePackages = "com.example.facotry")
public class SpringConsumerApplication {

    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);


    public static void main(String[] args) {
        SpringApplication.run(SpringConsumerApplication.class, args);
    }


    /*
        containerFactory 옵션을 커스텀 컨테이너로 설정한다.
        빈 객체로 등록된 커스텀 리스너 컨테이너 팩토리의 이름을 옵션값으로 설정하면 커스텀 리스너 컨테이너를 사용할 수 있다.
     */
    @KafkaListener(topics = "test",
            groupId = "test-group",
            containerFactory = "customContainerFactory")
    public void customListner(String data) {
        logger.info(data);
    }
}
