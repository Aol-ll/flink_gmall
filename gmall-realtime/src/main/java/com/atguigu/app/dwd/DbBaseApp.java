package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DbSplitProcessFunction;
import com.atguigu.app.func.DimSink;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author Aol
 * @create 2021-02-22 11:42
 */
public class DbBaseApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境,设置并行度,开启ck,设置状态后端
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //为节省资源暂不添加
/*        env.enableCheckpointing(6000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        env.getCheckpointConfig().setCheckpointTimeout(1000L);*/

        //获取kafka中数据
        FlinkKafkaConsumer<String> kafkaSourse = MyKafkaUtil.getKafkaSourse("ods_base_db_m", "ods_db_group");
        DataStreamSource<String> kafkaSouseDS = env.addSource(kafkaSourse);

        //将数据转化为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaSouseDS.map(JSON::parseObject);

        //对data为null的数据进行过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjectDS.filter(data -> data.getString("data") != null && data.getString("data").length() > 0);
        //filterDS.print();

        //分流
        //指定测输出流
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};
        //过滤获得kafka数据流
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = filterDS.process(new DbSplitProcessFunction(hbaseTag));
        //获取hbase数据流
        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        //将数据分别写入kafka和phoenix
        hbaseJsonDS.addSink(new DimSink());
        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                return new ProducerRecord<byte[], byte[]>(jsonObject.getString("sink_table")
                        ,jsonObject.getString("data").getBytes());
            }
        });
        kafkaJsonDS.addSink(kafkaSinkBySchema);
        //执行环境
        env.execute();
    }
}
