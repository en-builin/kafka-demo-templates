package en.builin.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    
    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    
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
        // create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, testMessage);
        
        // send the data - async
        producer.send(producerRecord);
        // flush the data - sync
        producer.flush();
        // flush & close the producer
        producer.close();
    }
}
