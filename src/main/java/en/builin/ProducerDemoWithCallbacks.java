package en.builin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallbacks {
    
    public static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class.getSimpleName());
    
    public static void main(String[] args) {
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            // create a producer record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("test_topic", "Test message " + i);
            // send the data - async
            producer.send(producerRecord, (metadata, e) -> {
                // executes every time a record successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    log.info("Received new metadata:\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
                            metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    log.error("Error while producing", e);
                }
            });
            
            // without a pause we have batching in StickyPartitioner
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("java.lang.InterruptedException", e);
                throw new RuntimeException(e);
            }
        }
        
        // flush the data - sync
        producer.flush();
        // flush & close the producer
        producer.close();
    }
}
