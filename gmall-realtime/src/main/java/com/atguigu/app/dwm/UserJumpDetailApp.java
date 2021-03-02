package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @author Aol
 * @create 2021-02-24 16:14
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        env.getCheckpointConfig().setCheckpointTimeout(60000L);*/

        //提取数据中的时间戳生成Watermark
        //老版本,默认使用的处理时间语义,新版本默认时间语义为事件时间
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //获取kafka中数据
        String sourceTopic = "dwd_page_log";
        String groupId = "userJumpDetailApp";
        String sinkTopic = "dwm_user_jump_detail";
        FlinkKafkaConsumer<String> kafkaSourse = MyKafkaUtil.getKafkaSourse(sourceTopic, groupId);
        DataStreamSource<String> kafkaSourseDS = env.addSource(kafkaSourse);
//        DataStream<String> kafkaSourseDS = env.socketTextStream("hadoop102", 9999);

        WatermarkStrategy<JSONObject> jsonObjectWatermarkStrategy = WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                });

        //转换数据类型并过滤
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaSourseDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("有问题");
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        }).assignTimestampsAndWatermarks(jsonObjectWatermarkStrategy);

        //按mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //定义cep
        Pattern<JSONObject, JSONObject> within = Pattern.<JSONObject>begin("start").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId == null || lastPageId.length() <= 0;
            }
        }).followedBy("follower").where(new IterativeCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject, Context<JSONObject> context) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return lastPageId != null && lastPageId.length() > 0;
            }
        }).within(Time.seconds(5));

        //作用到流上
        PatternStream<JSONObject> pattern = CEP.pattern(keyedStream, within);

        //差分流
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut") {
        };
        SingleOutputStreamOperator<Object> flatSelect = pattern.flatSelect(timeOutTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        collector.collect(map.get("start").get(0).toString());
                    }
                }, new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {

                    }
                });
        //输出流到kafka
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        flatSelect.getSideOutput(timeOutTag).addSink(kafkaSink);
        flatSelect.getSideOutput(timeOutTag).print();

        //执行任务
        env.execute();

    }
}
