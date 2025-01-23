import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ProducerCallback implements Callback {

    private final static Logger logger = LoggerFactory.getLogger(ProducerCallback.class);

    // 레코드의 비동기 결과를 받기 위해 사용. 만약 브로커 적재에 이슈가 생겼을 경우 Exception에 어떤 에러가 발생하였는지 매개변수로 전달
    // 데이터의 순서가 역전될 수 있으므로, 순서가 중요한 경우 사용하면 안 된다.
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if(e != null) {
            logger.error(e.getMessage(), e);
        } else {
            logger.info(recordMetadata.toString());
        }
    }
}
