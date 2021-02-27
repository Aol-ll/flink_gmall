package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentInfo;
import com.atguigu.bean.PaymentWide;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author Aol
 * @create 2021-02-27 14:13
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //开启ck，设置状态后端，超出时间
  /*      env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/
        //TODO 获取kafa数据
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        FlinkKafkaConsumer<String> payKafkaSource = MyKafkaUtil.getKafkaSourse(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> payKafkaDS = env.addSource(payKafkaSource);

        FlinkKafkaConsumer<String> orderWideKafkaSource = MyKafkaUtil.getKafkaSourse(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideKafkaDS = env.addSource(orderWideKafkaSource);

        //转化kafka数据为JavaBean、并指定watermark、按key分组
        WatermarkStrategy<PaymentInfo> paymentInfoWatermarkStrategy = WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        return DateTimeUtil.toTs(element.getCallback_time());
                    }
                });
        WatermarkStrategy<OrderWide> orderWideWatermarkStrategy = WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @SneakyThrows
                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {
                        return DateTimeUtil.toTs(element.getCreate_time());
                    }
                });
        KeyedStream<PaymentInfo, Long> paymentInfoDS = payKafkaDS
                .map(json -> JSON.parseObject(json, PaymentInfo.class))
                .assignTimestampsAndWatermarks(paymentInfoWatermarkStrategy).keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideDS = orderWideKafkaDS
                .map(json -> JSON.parseObject(json, OrderWide.class))
                .assignTimestampsAndWatermarks(orderWideWatermarkStrategy).keyBy(OrderWide::getOrder_id);

        //双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.intervalJoin(orderWideDS)
                .between(Time.seconds(-1800), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left,right));
                    }
                }).uid("payment_wide_join");

//        paymentWideDS.print();
        //写入kafka
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));
        //执行任务
        env.execute();

    }
}
