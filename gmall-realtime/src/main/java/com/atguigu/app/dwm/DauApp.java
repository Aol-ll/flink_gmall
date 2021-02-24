package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @author Aol
 * @create 2021-02-24 11:27
 */
public class DauApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
/*        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        env.getCheckpointConfig().setCheckpointTimeout(60000L);*/
        //获取kafka中dwd_page_log数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSourse = MyKafkaUtil.getKafkaSourse(sourceTopic, groupId);
        DataStreamSource<String> kafkaSourseDS = env.addSource(kafkaSourse);
        //转换为JSONObject对象流、分流脏数据
        SingleOutputStreamOperator<JSONObject> pageJSONDS = kafkaSourseDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        });
        //按照mid分组
        KeyedStream<JSONObject, String> pageDS = pageJSONDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //过滤掉不是今天第一次访问的数据
        SingleOutputStreamOperator<JSONObject> filter = pageDS.filter(new UvRichFilterFunction());
        //写到kafka
        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(sinkTopic);
        filter.map(JSON::toString).addSink(kafkaSink);
        //执行程序
        env.execute();
    }

    public static class UvRichFilterFunction extends RichFilterFunction<JSONObject> {

        private SimpleDateFormat simpleDateFormat;
        private ValueState<String> firstDateState;

        @Override
        public void open(Configuration parameters) throws Exception {
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

            ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("first-value", String.class);
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build();
            stringValueStateDescriptor.enableTimeToLive(stateTtlConfig);
            firstDateState = getRuntimeContext().getState(stringValueStateDescriptor);
        }

        @Override
        public boolean filter(JSONObject value) throws Exception {

            String last_page = value.getJSONObject("page").getString("last_page_id");
            if (last_page == null || last_page.length() <= 0) {

                Long ts = value.getLong("ts");
                String curDate = simpleDateFormat.format(ts);
                String firstDate = firstDateState.value();
                if (firstDate == null || !firstDate.equals(curDate)) {
                    firstDateState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }

        }
    }
}
