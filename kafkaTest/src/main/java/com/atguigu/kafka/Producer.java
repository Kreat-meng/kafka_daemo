package com.atguigu.kafka;

import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    private static ProducerRecord<Integer, String> first;

    public static void main(String[] args) {

        Properties prope = new Properties();

        prope.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092");
        prope.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        prope.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prope.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.atguigu.kafka.Partition");

        KafkaProducer<Integer, String> produce = new KafkaProducer<>(prope);

        for (int i = 0; i < 60; i++) {
            Integer a = (int)(Math.random()*100);
            System.out.println(a);
            first = new ProducerRecord<>("first",a,"lalalla,xiaohuaxhu");
            produce.send(first, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("消息：" + first.value() + " 主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition() + " 偏移量："
                                + recordMetadata.offset());
                    } else if (e != null) {
                        e.printStackTrace();
                    }
                }
            });
        }

        produce.close();
    }
}
