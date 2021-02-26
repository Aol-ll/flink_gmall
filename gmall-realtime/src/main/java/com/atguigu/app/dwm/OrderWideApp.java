package com.atguigu.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.OrderDetail;
import com.atguigu.bean.OrderInfo;
import com.atguigu.bean.OrderWide;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author Aol
 * @create 2021-02-26 10:42
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
//        //开启CK
//        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000L);


        //读取Kafka订单和订单明细主题数据 dwd_order_info  dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";


        FlinkKafkaConsumer<String> orderInfokafkaSourse = MyKafkaUtil.getKafkaSourse(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(orderInfokafkaSourse);
        FlinkKafkaConsumer<String> orderDetailKafkaSourse = MyKafkaUtil.getKafkaSourse(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(orderDetailKafkaSourse);
        //转化JavaBean、指定watermark
        WatermarkStrategy<OrderInfo> orderInfoWatermarkStrategy = WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                    @Override
                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                });
        WatermarkStrategy<OrderDetail> orderDetailWatermarkStrategy = WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                    @Override
                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                        return element.getCreate_ts();
                    }
                });


        KeyedStream<OrderInfo, Long> orderInfoKeyedStream = orderInfoKafkaDS.map(json -> {
            OrderInfo orderInfo = JSON.parseObject(json, OrderInfo.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String createTime = orderInfo.getCreate_time();
            String[] createTimeArr = createTime.split(" ");

            orderInfo.setCreate_date(createTimeArr[0]);
            orderInfo.setCreate_hour(createTimeArr[1]);
            orderInfo.setCreate_ts(sdf.parse(createTime).getTime());

            return orderInfo;
        }).assignTimestampsAndWatermarks(orderInfoWatermarkStrategy).keyBy(OrderInfo::getId);

        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailKafkaDS.map(json -> {
            OrderDetail orderDetail = JSON.parseObject(json, OrderDetail.class);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
            return orderDetail;
        }).assignTimestampsAndWatermarks(orderDetailWatermarkStrategy).keyBy(OrderDetail::getOrder_id);


        //双流join
        SingleOutputStreamOperator<OrderWide> orderWideStream = orderInfoKeyedStream.intervalJoin(orderDetailKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
        orderWideStream.print();
        //关联维度

        //写入kafka

        //执行任务
        env.execute();
    }
}
