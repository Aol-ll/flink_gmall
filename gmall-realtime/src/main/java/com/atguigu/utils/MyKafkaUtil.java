package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Aol
 * @create 2021-02-20 16:13
 */
public class MyKafkaUtil {

    private static String KAFKA_PORT = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    private static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers", KAFKA_PORT);
    }


    public static FlinkKafkaConsumer<String> getKafkaSourse(String topic, String goupId) {
        //给配置信息对象添加配置项
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, goupId);
        //获取KafkaSource
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        //获取KafkaSourceSink
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }
}
