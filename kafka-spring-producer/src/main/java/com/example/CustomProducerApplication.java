package com.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@SpringBootApplication
public class CustomProducerApplication implements CommandLineRunner {

    private static String TOPIC_NAME = "test";

    @Autowired
    private KafkaTemplate<String, String> customKafkaTemplate;

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(CustomProducerApplication.class);
        application.run(args);
    }

    @Override
    public void run(String... args) throws Exception {

        /*
            ListenableFuture - 데이터를 전송한 이후 정상 적재되었는지 여부를 확인하고 싶을 때 사용.
            ListenableFuture 인스턴스에 addCallback 함수를 붙여 프로듀서가 보낸 데이터의 브로커 적재 여부를 비동기로 확인할 수 있다.
                - onSuccess: 브로커에 정상 적재
                - onFailure: 적재되지 않고 이슈가 발생
         */
        ListenableFuture<SendResult<String, String>> future = customKafkaTemplate.send(TOPIC_NAME, "data");
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> stringStringSendResult) {

            }

            @Override
            public void onFailure(Throwable throwable) {

            }
        });

        System.exit(0);
    }
}
