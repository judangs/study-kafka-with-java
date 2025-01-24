package sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class SingleFileSinkTask extends SinkTask {

    private SingleFileSinkConnectorConfig config;
    private File file;
    private FileWriter writer;


    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {

        try {
            config = new SingleFileSinkConnectorConfig(props);
            file = new File(config.getString(config.DIR_FILE_NAME));
            writer = new FileWriter(file, true);
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }

    }

    /*
        일정 주기로 토픽의 데이터를 가져오는 put() 메서드에는 데이터를 저장하는 코드를 작성
        SinkRecord: 토픽의 레코드 (토픽, 파티션, 타임스탬프)
     */
    @Override
    public void put(Collection<SinkRecord> records) {

        try {
            for(SinkRecord record : records) {
                writer.write(record.value().toString() + "\n");
            }
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    /*
        파일에 데이터를 저장하는 것은 버퍼에 데이터를 저장하는 것.
        실질적으로 파일 시스템에 데이터를 저장하기 위해.
     */
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        try {
            writer.flush();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        try {
            writer.close();
        } catch (IOException e) {
            throw new ConnectException(e.getMessage(), e);
        }
    }
}
