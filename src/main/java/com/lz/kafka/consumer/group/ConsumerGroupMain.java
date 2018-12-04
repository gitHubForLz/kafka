package com.lz.kafka.consumer.group;



public class ConsumerGroupMain {
    private static String url = "192.168.128.128:9092,192.168.128.130:9092,192.168.128.132:9092";

    public static void main(String[] args){
        String brokers = url;
        String groupId = "group01";
        String topic = "HelloWorld";
        int consumerNumber = 3;

        Thread producerThread = new Thread(new ProducerThread(brokers,topic));
        producerThread.start();

        ConsumerGroup consumerGroup = new ConsumerGroup(brokers,groupId,topic,consumerNumber);
        consumerGroup.start();
    }
}