package simple;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;
import java.util.Map;

/*
    실제로 데이터를 다루는 클래스
        - 소스 애플리케이션 또는 소스 파일로부터 데이터를 가져와 토픽으로 보내는 역할을 수행한다.
        - 토픽에서 사용하는 오프셋이 아닌 자체적으로 사용하는 오프셋을 사용한다. (오프셋 스토리지에 저장)
        ※ Task가 사용하는 오프셋 - 소스 애플리케이션 또는 소스 파일을 어디까지 읽었는지 저장하기 위한 역할


 */
public class TestSourceTask extends SourceTask {
    @Override
    public String version() {
        return "";
    }

    @Override
    public void start(Map<String, String> map) {

    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        return List.of();
    }

    @Override
    public void stop() {

    }
}
