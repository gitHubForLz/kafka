package com.lz.kafka.simple;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    private static String url = "192.168.128.128:9092,192.168.128.130:9092,192.168.128.132:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", url);
        props.put("acks", "all");
        props.put("delivery.timeout.ms", 300000);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("transactional.id", "my-transactional-id");
        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(props);

        producer.initTransactions();
        producer.beginTransaction();
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("my-topic-default", Integer.toString(i), Integer.toString(i)));
        producer.commitTransaction();
        producer.close();
    }
}
