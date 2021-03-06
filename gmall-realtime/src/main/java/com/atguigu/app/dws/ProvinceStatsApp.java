package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Aol
 * @create 2021-03-05 17:48
 */
public class ProvinceStatsApp {
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

        //1.3 获取表的执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //2.读取Kafka数据创建动态表
        String sourceTopic = "dwm_order_wide";
        String groupId = "province_stats_app";

        tableEnv.executeSql(
                "CREATE TABLE ORDER_WIDE (" +
                        "province_id BIGINT, " +
                        "province_name STRING," +
                        "province_area_code STRING," +
                        "province_iso_code STRING," +
                        "province_3166_2_code STRING,order_id STRING, " +
                        "split_total_amount DOUBLE," +
                        "create_time STRING," +
                        "rowtime AS TO_TIMESTAMP(create_time,'yyyy-MM-dd HH:mm:ss')," +
                        "WATERMARK FOR  rowtime  AS rowtime )" +
                        MyKafkaUtil.getKafkaDDL(sourceTopic, groupId));
        //3.分组、开窗、聚合
        Table reduceTable = tableEnv.sqlQuery(
                "SELECT DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt," +
                        " DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt," +
                        " province_id," +
                        " province_name," +
                        " province_area_code," +
                        " province_iso_code," +
                        " province_3166_2_code," +
                        " sum(split_total_amount) order_amount," +
                        " count(*) order_count," +
                        " UNIX_TIMESTAMP()*1000 ts " +
                        " from ORDER_WIDE " +
                        " group by province_id,province_name,province_area_code,province_iso_code,province_3166_2_code,TUMBLE(rowtime, INTERVAL '10' SECOND)");

        reduceTable.execute().print();
        //4.将动态表转换为追加流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(reduceTable, ProvinceStats.class);

        //5.写入ClickHouse  province_stats_200821
        provinceStatsDS.addSink(ClickHouseUtil.getJdbcSink("insert into province_stats_2021 values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
