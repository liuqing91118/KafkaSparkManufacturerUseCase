package KafkaProducer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import com.google.gson.Gson;
import java.util.Properties;
import EventClass.Conveyor;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Created by qingl on 10/12/15.
 */
public class KafkaProducer {
    private final Producer<String, String> kafkaProducer;

    public KafkaProducer(String broker) {
        Properties props = new Properties();
        props.put("metadata.broker.list", broker);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("partitioner.class", "KafkaProducer.SimplePartitioner");
        props.put("request.required.acks", "1");


        ProducerConfig config = new ProducerConfig(props);

        kafkaProducer = new Producer<String, String>(config);
    }


    public void publish(String topic, SpecificRecordBase event) {
        try {
            Gson gson = new Gson();

            String json = gson.toJson(event);
            KeyedMessage<String, String> data = new KeyedMessage<String, String> (topic, json);
            kafkaProducer.send(data);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
