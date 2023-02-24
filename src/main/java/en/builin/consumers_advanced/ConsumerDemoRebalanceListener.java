package en.builin.consumers_advanced;

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

/**
 * manual committing to async committing
 * this pattern useful if we are committing offsets to custom store
 * another use is flushing cache of received data (i.e. we have data of one user in cache and after rebalancing
 * we can get other partition attached to consumer, so we need to flush cache for another client consumer
 */
public class ConsumerDemoRebalanceListener {
    
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoRebalanceListener.class.getSimpleName());
    
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
        // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        
        // listener with offset tracking
        ConsumerRebalanceListenerImpl listener = new ConsumerRebalanceListenerImpl(consumer);
        
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
            // subscribe consumer to our topics
            consumer.subscribe(Collections.singletonList(topicName));
            
            while (true) {
                // poll all messages Kafka has for now, but if there are no message, will wait for duration
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumedRecord : records) {
                    log.info("Key: {}, Value: {}", consumedRecord.key(), consumedRecord.value());
                    log.info("  Partition: {}, Offset: {}", consumedRecord.partition(), consumedRecord.offset());
                    // track the offset we commited to listener 
                    listener.addOffsetToTrack(consumedRecord.topic(), consumedRecord.partition(), consumedRecord.offset());
                }
                // we processed all data, and we don't want to block
                consumer.commitAsync();
            }
        } catch (WakeupException e) {
            // ignore this expected exception when closing consumer
            log.info("Consumer shutdown");
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
            try {
                // sync here tracked offsets
                consumer.commitSync(listener.getCurrentOffsets());
            } finally {
                consumer.close();
                log.info("Consumer closed");
            }
        }
    }
}
