package org.example.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Desription:
 *
 * @ClassName MyKafkaUtil
 * @Author Zhanyuwei
 * @Date 2021/11/4 10:55 下午
 * @Version 1.0
 **/
public class MyKafkaUtil {

    private static String brokers = "jtbihdp03:9092";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){

        return new FlinkKafkaProducer<>(brokers, topic, new SimpleStringSchema());
    }

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic,String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);

        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }
}
