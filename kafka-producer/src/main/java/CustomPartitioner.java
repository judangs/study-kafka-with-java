import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

// producer 애플리케이션의 ProducerRecord를 전송할 경우 topic의 partition에 매칭해주는 파티셔너
/*
    // 파티셔너를 따로 생성하지 않을 경우 : DefaultPartitoner로 설정
    구분: 둘 다 메시지 키가 있을 때는 메시지 키의 해시값과 파티션을 매칭하여 데이터를 전송한다는 점이 동일하다.
        1. UniformStickyPartitioner
            프로듀서 동작에 특화되어 높은 처리량과 낮은 리소스 사용률을 가지는 특징 => 기본적으로 배치로 묶어 전송한다.
        2. RoundRobinPartitioner
            레코드가 들어오는 대로 파티션을 순회하면서 전송한다.
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {

        if(bytes == null) {
            throw new InvalidRecordException("Need message key");
        }
        if(((String) key).equals("record-key")) {
            return 0;
        }

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(bytes)) % numPartitions;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
