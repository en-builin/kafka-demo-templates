package en.builin.consumers_advanced;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * read specific messages from specific partitions
 * replay data from a specific offset
 * = remove group.id, don't subscribe to topic, use assign() and seek() API
 * This demo assigns to 0 pertition of topic and get 5 messages starting from offset #7 
 */
public class ConsumerDemoAssignSeek {
    
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getSimpleName());
    
    public static void main(String[] args) {
        
        String bootstrapServers = "127.0.0.1:9092";
        String topicName = "test_topic";
        
        // create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // none - don't start of none previous offset found, earliest - from very beginning, latest - from now
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        
        TopicPartition partitionToReadFrom = new TopicPartition(topicName, 0);
        long offsetToReadFrom = 7L;
        // assign
        consumer.assign(List.of(partitionToReadFrom));
        // seek
        consumer.seek(partitionToReadFrom, offsetToReadFrom);
        
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesSoFar = 0;
        
        while (keepOnReading) {
            // poll all messages Kafka has for now, but if there are no message, will wait for duration
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> consumedRecord : records) {
                numberOfMessagesSoFar++;
                log.info("Key: {}, Value: {}", consumedRecord.key(), consumedRecord.value());
                log.info("  Partition: {}, Offset: {}", consumedRecord.partition(), consumedRecord.offset());
                if (numberOfMessagesSoFar >= numberOfMessagesToRead) {
                    // both loops exit
                    keepOnReading = false;
                    break;
                }
            }
        }
        
        consumer.close();
    }
}
