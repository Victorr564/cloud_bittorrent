package com.microservice.parser.presentation.config;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class KafkaProducerComponent {
    private static Properties props=new Properties();
    private Producer<String, String> producer = null;
    private KafkaProducerComponent()
    {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS","localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
    public void sendRecord(String topic,String key,String message)
    {
        try {
            // Create the producer
            producer = new KafkaProducer<>(props);
            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,message);

            // Send the record with a callback to handle exceptions
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.err.println("Error sending record: " + exception.getMessage());
                        exception.printStackTrace();
                    } else {
                        System.out.println("Record sent successfully to topic " + metadata.topic() +
                                " partition " + metadata.partition() + " at offset " + metadata.offset());
                    }
                }
            });

        } catch (Exception e) {
            System.err.println("Error creating or sending producer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the producer
            if (producer != null) {
                try {
                    producer.close();
                } catch (Exception e) {
                    System.err.println("Error closing producer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}
