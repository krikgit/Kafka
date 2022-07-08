package self.learn;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("ProducerDemoWithCallback : Hi ");

        final String TOPIC_NAME = "third_topic";
        final String MESSAGE = "Msg inserted by java ProducerDemoWithCallback class";
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
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    log.info("Received new metadata\n"+
                            "\nTopic :"+recordMetadata.topic()+
                            "\nPartition:"+recordMetadata.partition()+
                            "\nTime:"+recordMetadata.timestamp());
                }else {
                    log.error("Exception:{}"+e);
                }
            }
        });

        //flush - close
        producer.flush();
        producer.close();
    }
}
