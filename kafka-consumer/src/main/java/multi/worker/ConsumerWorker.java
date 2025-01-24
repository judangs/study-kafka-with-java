package multi.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWorker implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerWorker.class);
    private String recordValue;

    ConsumerWorker(String recordValue) {
        this.recordValue = recordValue;
    }

    @Override
    public void run() {
        logger.info("thread:{} \t record: {}", Thread.currentThread().getName(), recordValue);
    }
}
