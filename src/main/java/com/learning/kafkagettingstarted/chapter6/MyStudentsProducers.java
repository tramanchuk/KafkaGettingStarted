package com.learning.kafkagettingstarted.chapter6;

import com.learning.kafkagettingstarted.chapter5.KafkaSimpleProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class MyStudentsProducers {
    public static void main(String[] args){

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        ProducerRecord<String, String> record1 = new ProducerRecord<>(
                "kafka.usecase.students",
                "1001",
                "My record 1");
        producer.send(record1);
        System.out.println("Sending Message : "+ record1.toString());
    }
}
