package com.atguigu.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author mengx
 *
 * 1. 实现接口Partitioner
 * 2. 实现3个方法:partition,close,configure
 * 3. 编写partition方法,返回分区号
 */


public class Partition implements Partitioner {


    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String s1 = o.toString();

        int parNum;

        int keyHash = s1.hashCode();

        Integer topic = cluster.partitionCountForTopic("first");

        parNum= Math.abs(keyHash)%topic;

        return parNum;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
