package multi.thread;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiConsumerThreadStrategy {

    private static final String TOPIC_NAME = "test";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test-group";
    private static final int CONSUMER_COUNT = 3;

    public static void main(String[] args) {

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ExecutorService executorService = Executors.newCachedThreadPool();
        for(int i=0; i<CONSUMER_COUNT; i++) {
            ConsumerWorker worker = new ConsumerWorker(configs, TOPIC_NAME, i);
            executorService.execute(worker);
        }

    }
}
