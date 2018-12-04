package com.lz.kafka.consumer.group;


import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class ConsumerGroup {
    private final String brokers;
    private final String groupId;
    private final String topic;
    private final int consumerNumber;
    private List<ConsumerThread> consumerThreadList = new ArrayList<ConsumerThread>();

    public ConsumerGroup(String brokers, String groupId, String topic, int consumerNumber) {
        this.groupId = groupId;
        this.topic = topic;
        this.brokers = brokers;
        this.consumerNumber = consumerNumber;

        for (int i = 0; i < consumerNumber; i++) {
            ConsumerThread consumerThread = new ConsumerThread(brokers, groupId, topic);
            consumerThreadList.add(consumerThread);
        }
    }

    private ConsumerRecords getPartition(String brokers, String groupId, String topic) {

        Properties properties = ConsumerThread.buildKafkaProperty(brokers, groupId);
        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));

        ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
        return records;


    }

    public void start() {
       /* //线程安全w
        Set<TopicPartition> partitions;
        ConsumerRecords<String, String> records0;
        while (true) {
            records0 = getPartition(brokers, groupId, topic);
            partitions = records0.partitions();
            if (partitions.size() != 0) {
                break;
            }
        }
        final ConsumerRecords records=records0;
        partitions.forEach((partition) -> {

            new Thread(() -> {
                while (true) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    System.out.println(partition.partition());
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        System.out.println(record.offset() + ": " + record.value());
                    }
                }
            }).start();
        });*/
        for (ConsumerThread item : consumerThreadList) {
            Thread thread = new Thread(item);
            thread.start();
        }
    }
// 获取所有分区
}