import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/*
    // 토픽의 파티션으로부터 데이터를 가져가기 위해 컨슈머를 운영하는 방법
        1. 1개 이상의 컨슈머로 이루어진 컨슈머 그룹을 운영하는 것
            - 1개의 파티션은 최대 1개의 컨슈머에 할당 가능. 1개의 컨슈머는 여러 개의 파티션에 할당될 수 있다.
            - 컨슈머 그룹의 컨슈머 개수는 가져가고자 하는 토픽의 파티션 개수보다 같거나 작아야 한다.
                => 컨슈머 개수가 많을 경우, 파티션을 할당받지 못한 컨슈머는 스레드만 차지하고 실질적인 데이터를 처리하지 못함.
        2. 토픽의 특정 파티션만 구독하는 컨슈머를 운영하는 것
 */
public class SimpleProducer {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";


    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "record-key", "message");

        // 동기로 프로듀서의 전송 결과를 확인하는 것은 빠른 전송에 허들이 될 수 있다.
        //  -> 브로커로부터 전송에 대한 응답 값을 받기 전까지 대기. 이 경우 비동기로 결과를 확인할 수 있도록
        //  사용자 정의 Callback 클래스(ProducerCallback.java)
//        RecordMetadata meta = producer.send(record).get();

        producer.send(record);
        producer.flush();
        producer.close();

    }
}
