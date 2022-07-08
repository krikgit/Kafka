package self.learn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        log.info("ProducerDemo : Hello ");

        final String TOPIC_NAME = "java_topic";
        final String MESSAGE = "Welcome to Kafka by Java";
        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";

        //Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //Producer record
        ProducerRecord<String,String> record =
                new ProducerRecord<>(TOPIC_NAME,MESSAGE);

        //Async- Send data
        producer.send(record);

        //flush - close
        producer.flush();
        producer.close();
    }
}
