package en.builin.consumers_advanced;

import org.apache.kafka.clients.consumer.Consumer;
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
import java.util.concurrent.CountDownLatch;

/**
 * running consumer in separate thread
 */
public class ConsumerDemoInThread {
    
    public static void main(String[] args) {
        ConsumerDemoWorker consumerDemoWorker = new ConsumerDemoWorker();
        new Thread(consumerDemoWorker).start();
        Runtime.getRuntime().addShutdownHook(new Thread(new ConsumerDemoCloser(consumerDemoWorker)));
    }

    private static class ConsumerDemoWorker implements Runnable {

        public static final Logger log = LoggerFactory.getLogger(ConsumerDemoInThread.class.getSimpleName());
        
        private CountDownLatch countDownLatch;
        private Consumer<String, String> consumer;
        
        @Override
        public void run() {
            
            countDownLatch = new CountDownLatch(1);
            
            String bootstrapServers = "127.0.0.1:9092";
            String groupId = "second-application";
            String topicName = "test_topic";

            // create consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(topicName));
            
            final Duration pollTimeout = Duration.ofMillis(100);
            
            try {
                while (true) {
                    final ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
                    for (final ConsumerRecord<String, String> consumedRecord : records) {
                        log.info("Key: {}, Value: {}", consumedRecord.key(), consumedRecord.value());
                        log.info("  Partition: {}, Offset: {}", consumedRecord.partition(), consumedRecord.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Consumer wake up for stopping");
            } finally {
                consumer.close();
                countDownLatch.countDown();
            }
            
        }
        
        void shutdown() throws InterruptedException {
            consumer.wakeup();
            countDownLatch.await();
            log.info("Consumer closed");
        }
    }
    
    private static class ConsumerDemoCloser implements Runnable {
        
        private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCloser.class.getName());
        
        private final ConsumerDemoWorker consumerDemoWorker;
        
        ConsumerDemoCloser(final ConsumerDemoWorker consumerDemoWorker) {
            this.consumerDemoWorker = consumerDemoWorker;
        }
        
        @Override
        public void run() {
            try {
                consumerDemoWorker.shutdown();
            } catch (InterruptedException e) {
                log.error("Error shutting down consumer", e);
            }
        }
    }
}
