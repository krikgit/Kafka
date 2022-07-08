package self.learn;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("ConsumerDemo : Hi");

        final String TOPIC_NAME = "third_topic";
        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        final String GROUP_ID = "my-first-consumer-group";


        //Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //adding shutdown
        final  Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("shutdown detected:");
                consumer.wakeup();
                try{
                    mainThread.join();
                }catch (InterruptedException e){
                    e.getStackTrace();
                }
            }
        });

        try{
            //subscribe
            consumer.subscribe(Arrays.asList(TOPIC_NAME));

            //poll data
            while(true){
                ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record: consumerRecord){
                    log.info("Key:"+record.key()+"--value:"+record.value());
                }

            }
        }catch (WakeupException exception){
            log.info("OK - Wake up exception");
        }catch (Exception e ){
            log.info("in Exception");
        }finally {
            consumer.close();
        }



    }
}
