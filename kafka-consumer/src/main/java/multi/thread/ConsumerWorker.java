package multi.thread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private Properties props;
    private String topic;
    private String threadName;

    /*
        KafkaConsumer 클래스는 스레드 세이프하지 않다. 때문에 스레드별로 KafkaConsumer 인스턴스를 별개로 만들어서 운영해야 한다.
        만약 KafkaConsumer 인스턴스를 여러 스레드에서 실행하면 ConcurrentModificationException 예외가 발생하게 된다.
     */
    private KafkaConsumer<String, String> consumer;

    ConsumerWorker(Properties props, String topic, int number) {
        this.props = props;
        this.topic = topic;
        this.threadName = "consumer-thread-" + number;

    }

    @Override
    public void run() {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for(ConsumerRecord<String, String> record: records) {
                logger.info("{}", record);
            }
        }
    }
}
