package com.atguigu.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @author Aol
 * @create 2021-02-20 21:31
 */
public class LogBaseApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境,设置并行度,开启ck,设置状态后端,设置超时时间,设置HDFS权限
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(60000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        env.getCheckpointConfig().setCheckpointTimeout(10000L);

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        //获取kafka数据
        FlinkKafkaConsumer<String> kafkaSourse = MyKafkaUtil.getKafkaSourse("ods_base_log", "dwd_log");
        DataStreamSource<String> kafkaSourseDS = env.addSource(kafkaSourse);
        //解析kafka数据为json
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaSourseDS.map(JSONObject::parseObject);
        //按mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(data -> data.getJSONObject("common").getString("mid"));
        //判断新老用户
        SingleOutputStreamOperator<JSONObject> isNewOrOldDS = keyedStream.map(new NewMidRichMapFunc());
        //将数据分别,分入page,displays,start流中
        SingleOutputStreamOperator<String> pageDS = isNewOrOldDS.process(new SplitProcessFunc());
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") {});
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display") {});
        //分入不同kafka的topic
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

/*        pageDS.print("page------");
        startDS.print("start---------");
        displayDS.print("display---------");*/
        //结束环境
        env.execute();
    }
    public static class NewMidRichMapFunc extends RichMapFunction<JSONObject,JSONObject>{

        private ValueState<String> fistDateTime;
        private SimpleDateFormat simpleDateFormat;

        @Override
        public void open(Configuration parameters) throws Exception {
            fistDateTime = getRuntimeContext().getState(new ValueStateDescriptor<String>("new-mid", String.class));
            simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            String isNew = value.getJSONObject("common").getString("is_new");

            if ("1".equals(isNew)){
                String time = fistDateTime.value();
                if (time != null){
                    value.getJSONObject("common").put("is_new", "0");
                }else {
                    fistDateTime.update(simpleDateFormat.format(value.getLong("ts")));
                }
            }
            return value;
        }
    }
    public static class SplitProcessFunc extends ProcessFunction<JSONObject,String>{
        @Override
        public void processElement(JSONObject jsonObject, Context context, Collector<String> collector) throws Exception {
            String startJSON = jsonObject.getString("start");
            JSONArray displaysJSON = jsonObject.getJSONArray("displays");
            if (startJSON != null && startJSON.length() > 0){
                context.output(new OutputTag<String>("start") {},jsonObject.toString());
            } else if (displaysJSON != null && displaysJSON.size() > 0) {
                for (int i = 0; i < displaysJSON.size(); i++) {
                    JSONObject display = displaysJSON.getJSONObject(i);
                    display.put("page", jsonObject.getJSONObject("page").getString("page_id"));
                    context.output(new OutputTag<String>("display") {},display.toString());
                }
            } else {
                collector.collect(jsonObject.toString());
            }
        }
    }
}
