package processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/*
    스트림 프로세서 클래스를 생성하기 위해서는 Processor 또는 Transformer 인터페이스를 사용해야 한다.
 */
public class FilterProcessor implements Processor<String, String> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(String key, String value) {
        if(value.length() > 5) {
            context.forward(key, value);
        }

        context.commit();
    }

    @Override
    public void close() {

    }
}
