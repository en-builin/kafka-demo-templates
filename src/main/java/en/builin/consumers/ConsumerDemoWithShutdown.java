package en.builin.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
    
    public static void main(String[] args) {
        
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "third-application";
        String topicName = "test_topic";
        
        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // none - don't start of none previous offset found, earliest - from very beginning, latest - from now
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        
        // get a reference to current thread
        final Thread mainThread = Thread.currentThread();
        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected...");
            // initialize exception
            consumer.wakeup();
            // join the main thread to allow the execution of the main thread code
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            while (true) {
                // subscribe consumer to our topics
                consumer.subscribe(Collections.singletonList(topicName));
                
                // poll all messages Kafka has for now, but if there are no message, will wait for duration
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumedRecord : records) {
                    log.info("Key: {}, Value: {}", consumedRecord.key(), consumedRecord.value());
                    log.info("  Partition: {}, Offset: {}", consumedRecord.partition(), consumedRecord.offset());
                }
            }
        } catch (WakeupException e) {
            // ignore this expected exception when closing consumer
            log.info("Consumer shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            // close gracefully and commit the offsets
            consumer.close();
            log.info("Consumer closed");
        }
    }
}
