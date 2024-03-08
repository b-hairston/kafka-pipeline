package com.kafka;
import static java.lang.System.*;

import java.util.*;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.*;


/**
 * A simple Kafka producer
 * Authoor: Imani
 */


public final class SimpleProducer{
    public static void main (String[] args)
        throws InterruptedException{
    
    // Define the topic to which we will publish
    final var topic = "getting-started";
    
    // create a topic
            
    // Define the configuration for the producer using a Map
    // TODO: sort out the port listening configuration
    final Map<String, Object> config =
            Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", // tell the producer where to connect to
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, // tell the producer how to serialize the key
            StringSerializer.class.getName(), // use the string serializer provided by Kafka
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, // tell the producer how to serialize the value
            StringSerializer.class.getName(), // use the string serializer provided by Kafka
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
            ProducerConfig.MAX_BLOCK_MS_CONFIG, 300000); // ensure that we don't push duplicates

    try (var producer = new KafkaProducer<String, String>(config)){  // create a new producer (KafkaProducer object)
        while(true){
            final var key = "myKey"; // the key of the record
            final var value = "myValue"; // the value of the record
            out.format("Publishing record with value %s%n", value); // log the record

    
    final Callback callback = (metadata, exception) -> {
        out.format("Published with metadata: %s, error: %s%n",
        metadata, exception);
        };
                

    producer.send(new ProducerRecord<>(topic, key, value), callback); // publish the record to the topic


    Thread.sleep(1000); // wait for a second
        }
    }
    }
}
