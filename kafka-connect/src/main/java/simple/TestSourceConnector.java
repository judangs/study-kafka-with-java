package simple;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

/*
    태스크를 실행하기 전 커넥터 설정파일을 초기화하고 어떤 태스크 클래스를 사용할 것인지 정의하는 데에 사용
    실질적인 데이터를 다루는 부분이 들어가지 않는다.
 */
public class TestSourceConnector extends SourceConnector {


    @Override
    public void start(Map<String, String> map) {

    }

    // 커넥터가 사용할 태스크 클래스를 지정한다.
    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    // 태스크 개수가 2개 이상인 경우 태스크마다 각기 다른 옵션을 설정할 때 사용
    @Override
    public List<Map<String, String>> taskConfigs(int i) {
        return List.of();
    }

    @Override
    public void stop() {

    }

    @Override
    public ConfigDef config() {
        return null;
    }

    // 커넥트에 포함된 커넥터 플러그인을 조회할 때 해당 버전이 노출. 커넥터를 지속적으로 유지보수하고 신규 배포할 때 이 메서드가 리턴하는 버전 값을 변경해야 한다.
    @Override
    public String version() {
        return "";
    }
}
