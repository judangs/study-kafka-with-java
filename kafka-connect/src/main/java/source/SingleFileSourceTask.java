package source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SingleFileSourceTask extends SourceTask {

    private Logger logger = LoggerFactory.getLogger(SingleFileSourceTask.class);

    // 파일 이름과 해당 파일을 읽은 지점을 표기하기 위한 필드.
    public final String FILENAME_FIELD = "filename";
    public final String POSITION_FIELD = "position";

    private Map<String, String> fileNamePartition;
    private Map<String, Object> offset;
    private String topic;
    private String file;
    private long position = -1;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {

        try {
            SingleFileSourceConnectorConfig config = new SingleFileSourceConnectorConfig(props);

            topic = config.getString(SingleFileSourceConnectorConfig.TOPIC_NAME);
            file = config.getString(SingleFileSourceConnectorConfig.DIR_FILE_NAME);

            fileNamePartition = Collections.singletonMap(FILENAME_FIELD, file);

            /*
                오프셋 스토리지에서 현재 읽고자 하는 파일 정보를 가져온다.
                    오프셋 스토리지
                        - 단일 모드 커넥트: 로컬 파일로 저장
                        - 분산 모드 커넥트: 카프카 내부 토픽
             */
            offset = context.offsetStorageReader().offset(fileNamePartition);

            if(offset != null) {
                Object lastReadFileOffset = offset.get(POSITION_FIELD);
                if (lastReadFileOffset != null) {
                    position = (Long) lastReadFileOffset;
                }
            } else {
                position = 0;
            }
        } catch (Exception e) {
            throw new ConnectException(e.getMessage(), e);
        }

    }

    /*
        태스크가 시작한 이후 지속적으로 데이터를 가져오기 위해 반복적으로 호출되는 메서드.
     */
    @Override
    public List<SourceRecord> poll() {

        List<SourceRecord> result = new ArrayList<>();
        try {
            Thread.sleep(1000);

            List<String> lines = getLines(position);

            if(lines.size() > 0) {
                lines.forEach(line -> {

                    Map<String, Long> sourceOffset = Collections.singletonMap(POSITION_FIELD, ++position);
                    SourceRecord sourceRecord = new SourceRecord(fileNamePartition, sourceOffset, topic, Schema.STRING_SCHEMA, line);
                    result.add(sourceRecord);

                });
            }

            return result;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new ConnectException(e.getMessage(), e);

        }
    }

    private List<String> getLines(long readLine) throws Exception {
        BufferedReader reader = Files.newBufferedReader(Paths.get(file));
        return reader.lines().skip(readLine).collect(Collectors.toList());
    }

    @Override
    public void stop() {

    }
}
