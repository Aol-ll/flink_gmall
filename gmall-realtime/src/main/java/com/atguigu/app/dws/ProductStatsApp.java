package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimAsyncFunction;
import com.atguigu.bean.OrderWide;
import com.atguigu.bean.PaymentWide;
import com.atguigu.bean.ProductStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.DateTimeUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author Aol
 * @create 2021-03-03 17:45
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/dwd_log/ck"));
        //开启CK
        //env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //获取kafka数据
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewDStream = env.addSource(MyKafkaUtil.getKafkaSourse(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorInfoDStream = env.addSource(MyKafkaUtil.getKafkaSourse(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartInfoDStream = env.addSource(MyKafkaUtil.getKafkaSourse(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideDStream = env.addSource(MyKafkaUtil.getKafkaSourse(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideDStream = env.addSource(MyKafkaUtil.getKafkaSourse(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundInfoDStream = env.addSource(MyKafkaUtil.getKafkaSourse(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoDStream = env.addSource(MyKafkaUtil.getKafkaSourse(commentInfoSourceTopic, groupId));

        //转换数据类型
        SingleOutputStreamOperator<ProductStats> clickAndDisplayDS = pageViewDStream.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                JSONObject pageObj = jsonObject.getJSONObject("page");
                String page_id = pageObj.getString("page_id");
                Long ts = jsonObject.getLong("ts");
                if ("good_detail".equals(page_id)) {
                    out.collect(ProductStats.builder()
                            .sku_id(pageObj.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displaysJSONObject = displays.getJSONObject(i);
                        String item_type = displaysJSONObject.getString("item_type");
                        if ("sku_id".equals(item_type)) {
                            out.collect(ProductStats.builder()
                                    .sku_id(displaysJSONObject.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .cart_ct(1L)
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> orderDS = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                OrderWide orderWide = JSON.parseObject(value, OrderWide.class);
                Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());
                return ProductStats.builder()
                        .sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getTotal_amount())
                        .orderIdSet(new HashSet<>(Collections.singleton(orderWide.getOrder_id())))
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                PaymentWide paymentWide = JSON.parseObject(value, PaymentWide.class);
                Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                return ProductStats.builder()
                        .sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getTotal_amount())
                        .paidOrderIdSet(new HashSet<>(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(new HashSet<>(Collections.singleton(jsonObject.getLong("order_id"))))
                        .ts(ts)
                        .build();
            }
        });

        SingleOutputStreamOperator<ProductStats> commentDS = commentInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                Long ts = DateTimeUtil.toTs(jsonObject.getString("create_time"));
                String appraise = jsonObject.getString("appraise");
                long goodCommentCt = 0L;
                if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                    goodCommentCt = 1L;
                }
                return ProductStats.builder()
                        .sku_id(jsonObject.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCommentCt)
                        .ts(ts)
                        .build();
            }
        });

        //union
        DataStream<ProductStats> union = clickAndDisplayDS.union(favorStatsDS, cartStatsDS, orderDS, paymentStatsDS, refundStatsDS, commentDS);
        //指定watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWaterMarkDS = union.assignTimestampsAndWatermarks(WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((e, s) -> e.getTs()));
        //分组、开窗、聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWaterMarkDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));

                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct((long)stats1.getOrderIdSet().size());
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());


                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct((long)stats1.getRefundOrderIdSet().size());
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct((long)stats1.getPaidOrderIdSet().size());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;

                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        ProductStats productStats = input.iterator().next();
                        long end = window.getEnd();
                        long start = window.getStart();
                        String edt = sdf.format(end);
                        String stt = sdf.format(start);

                        productStats.setEdt(edt);
                        productStats.setStt(stt);
                        out.collect(productStats);
                    }
                });
        //关联维度
        //TODO 6.补充商品维度信息
        //6.1 补充SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDstream =
                AsyncDataStream.unorderedWait(reduceDS,
                        new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSku_id());
                            }

                            @Override
                            public void join(JSONObject jsonObject, ProductStats productStats) throws Exception {
                                productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                                productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                                productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                                productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                                productStats.setTm_id(jsonObject.getLong("TM_ID"));
                            }
                        }, 300, TimeUnit.SECONDS);

        //6.2 补充SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDstream =
                AsyncDataStream.unorderedWait(productStatsWithSkuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getSpu_id());
                            }

                            @Override
                            public void join(JSONObject jsonObject, ProductStats productStats) throws Exception {
                                productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                            }
                        }, 300, TimeUnit.SECONDS);


        //6.3 补充品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3Dstream =
                AsyncDataStream.unorderedWait(productStatsWithSpuDstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getCategory3_id());
                            }

                            @Override
                            public void join(JSONObject dimInfo, ProductStats input) throws Exception {
                                input.setCategory3_name(dimInfo.getString("NAME"));
                            }
                        }, 300, TimeUnit.SECONDS);

        //6.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDstream =
                AsyncDataStream.unorderedWait(productStatsWithCategory3Dstream,
                        new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }

                            @Override
                            public void join(JSONObject jsonObject, ProductStats productStats) throws Exception {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }
                        }, 300, TimeUnit.SECONDS);

        productStatsWithTmDstream.print();
        //写入clickhouse
        productStatsWithTmDstream.addSink(ClickHouseUtil.<ProductStats>getJdbcSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
        //执行任务

        env.execute();
    }
}
