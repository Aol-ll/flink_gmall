package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author Aol
 * @create 2021-02-20 16:13
 */
public class MyKafkaUtil {

    private static final String KAFKA_PORT = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static final String DEFAULT_TOPIC = "dwd_default_topic";

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
    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDL(String topic,String groupId) {

        return  "   WITH (  'connector' = 'kafka'," +
                "  'topic' = '" + topic + "'," +
                "  'properties.bootstrap.servers' = '" + KAFKA_PORT + "'," +
                "  'properties.group.id' = '" + groupId + "'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json' )";
    }
}
