package com.lz.kafka.simple;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class Consumer {
    private static String url = "192.168.128.128:9092,192.168.128.130:9092,192.168.128.132:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", url);
        properties.put("group.id", "4035");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("my-topic-default"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
              /*  Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.offset()+":"+record.value());
                }*/
                Set<TopicPartition> partitions = records.partitions();
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    System.out.println(partition.partition());
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }
}
