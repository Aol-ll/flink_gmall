package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.VisitorStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @author Aol
 * @create 2021-03-01 13:13
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
//        //开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //读取kafka数据
        // dwd_page_log(pv,访问时长,进入页面数)
        // dwm_unique_visit(uv)
        // dwm_user_jump_detail (跳出数)
        String groupId = "visitor_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";
        DataStreamSource<String> pageLogDS = env.addSource(MyKafkaUtil.getKafkaSourse(pageViewSourceTopic, groupId));
        DataStreamSource<String> uvDS = env.addSource(MyKafkaUtil.getKafkaSourse(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpDS = env.addSource(MyKafkaUtil.getKafkaSourse(userJumpDetailSourceTopic, groupId));
        //测试
/*        pageLogDS.print("page>>>>>>>>");
        uvDS.print("uv>>>>>>>>>>>>>>>");
        userJumpDS.print("jump>>>>>>>");*/
        //转换数据类型为JavaBean
        SingleOutputStreamOperator<VisitorStats> pvAndDurDS = pageLogDS.map(jsonStr -> {
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    0L,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> svCountDS = pageLogDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<VisitorStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");
                if (lastPage == null || lastPage.length() <= 0) {
                    JSONObject common = jsonObject.getJSONObject("common");
                    out.collect(new VisitorStats(
                            "",
                            "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L,
                            0L,
                            1L,
                            0L,
                            0L,
                            jsonObject.getLong("ts")));
                }
            }
        });

        SingleOutputStreamOperator<VisitorStats> uvCountDS = uvDS.map(jsonStr -> {
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> ujCountDS = userJumpDS.map(jsonStr -> {
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONObject common = jsonObject.getJSONObject("common");
            return new VisitorStats(
                    "",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //对数据进行union
        DataStream<VisitorStats> unionDS = pvAndDurDS.union(svCountDS, uvCountDS, ujCountDS);
        //分组
        WatermarkStrategy<VisitorStats> visitorStatsWatermarkStrategy = WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((e, s) -> e.getTs());
        SingleOutputStreamOperator<VisitorStats> visitorStatsSingleOutputStreamOperator = unionDS.assignTimestampsAndWatermarks(visitorStatsWatermarkStrategy);

        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = visitorStatsSingleOutputStreamOperator.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return Tuple4.of(value.getVc(), value.getCh(), value.getAr(), value.getIs_new());
            }
        });

        //开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> window
                = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<VisitorStats> result = window.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                return new VisitorStats(
                        "",
                        "",
                        value1.getVc(),
                        value1.getCh(),
                        value1.getAr(),
                        value1.getIs_new(),
                        value1.getUv_ct()+value2.getUv_ct(),
                        value1.getPv_ct()+value2.getPv_ct(),
                        value1.getSv_ct()+value2.getSv_ct(),
                        value1.getUj_ct()+value2.getUj_ct(),
                        value1.getDur_sum()+value2.getDur_sum(),
                        System.currentTimeMillis());
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                long start = window.getStart();
                long end = window.getEnd();
                String stt = sdf.format(start);
                String edt = sdf.format(end);
                VisitorStats visitorStats = input.iterator().next();
                visitorStats.setStt(stt);
                visitorStats.setEdt(edt);

                out.collect(visitorStats);

            }
        });

        result.print();
        //写入ClickHouse
        result.addSink(ClickHouseUtil.<VisitorStats>getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        //执行任务
        env.execute();
    }
}
