package com.lz.kafka.consumer.pool;


import com.lz.kafka.consumer.group.ProducerThread;

/**
 * Author  : RandySun (sunfeng152157@sina.com)
 * Date    : 2017-08-20  16:49
 * Comment :
 */
public class ConsumerThreadMain {
    private static String url = "192.168.128.128:9092,192.168.128.130:9092,192.168.128.132:9092";
    public static void main(String[] args){
        String brokers =url;
        String groupId = "group01";
        String topic = "HelloWorld123";
        int consumerNumber = 3;


        Thread producerThread = new Thread(new ProducerThread(brokers,topic));
        producerThread.start();

        ConsumerThread consumerThread = new ConsumerThread(brokers,groupId,topic);
        consumerThread.start(3);


    }
}