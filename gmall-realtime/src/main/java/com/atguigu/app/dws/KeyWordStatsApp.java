package com.atguigu.app.dws;


import com.atguigu.app.func.KeyWordUDTF;
import com.atguigu.bean.KeywordStats;
import com.atguigu.common.GmallConstant;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Aol
 * @create 2021-03-05 18:31
 */
public class KeyWordStatsApp {
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

        //2.读取Kafka主题数据创建动态表  dwd_page_log
        String sourceTopic = "dwd_page_log";
        String groupId = "keyword_stats_app";

        tableEnv.executeSql(
                "CREATE TABLE page_view " +
                        " (common MAP<STRING,STRING>, " +
                        " page MAP<STRING,STRING>," +
                        " ts BIGINT, " +
                        " rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                        " WATERMARK FOR  rowtime  AS  rowtime - INTERVAL '2' SECOND) " +
                        MyKafkaUtil.getKafkaDDL(sourceTopic, groupId));
        //3.过滤数据,只需要搜索数据,搜索的关键词不能为空
        Table filterTable = tableEnv.sqlQuery(
                "select " +
                        " page['item'] fullWord," +
                        " rowtime " +
                        " from page_view " +
                        " where page['item_type'] = 'keyword' and page['item'] is not null");

        //4.使用UDTF函数进行切词处理  函数注册
        tableEnv.createTemporarySystemFunction("ik_analyze", KeyWordUDTF.class);
        Table analyzeWordTable = tableEnv.sqlQuery(
                "select" +
                        " word, " +
                        " rowtime " +
                        " from " + filterTable + " ,LATERAL TABLE (ik_analyze(fullWord)) as T(word)");

        //5.分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery(
                "select " +
                        " word, " +
                        " count(*) ct, " +
                        " '" + GmallConstant.KEYWORD_SEARCH + "' source ," +
                        " DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                        " DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                        " UNIX_TIMESTAMP()*1000 ts" +
                        " from " + analyzeWordTable +
                        " group by TUMBLE( rowtime , INTERVAL '10' SECOND), word ");

        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();

        //6.将数据写入ClickHouse
        keywordStatsDataStream.addSink(ClickHouseUtil.getJdbcSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //7.执行任务
        env.execute();
    }
}
