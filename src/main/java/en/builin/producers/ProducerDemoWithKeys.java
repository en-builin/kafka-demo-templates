package en.builin.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    
    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    
    public static void main(String[] args) {
        
        String bootstrapServers = "localhost:9092";
        String topicName = "test_topic";
        String testMessage = "Test message";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {

            String key = "id_" + i;
            String value = testMessage + i;
            
            // create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
            // send the data - async
            producer.send(producerRecord, (metadata, e) -> {
                // executes every time a record successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    log.info("Received new metadata:\nTopic: {}\nKey: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                            metadata.topic(), producerRecord.key(), metadata.partition(), metadata.offset(), 
                            metadata.timestamp());
                } else {
                    log.error("Error while producing", e);
                }
            });
        }
        
        // flush the data - sync
        producer.flush();
        // flush & close the producer
        producer.close();
    }
}
