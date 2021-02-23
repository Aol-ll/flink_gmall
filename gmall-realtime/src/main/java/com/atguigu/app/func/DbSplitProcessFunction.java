package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Aol
 * @create 2021-02-23 11:35
 */
public class DbSplitProcessFunction extends ProcessFunction<JSONObject,JSONObject> {

    private OutputTag<JSONObject> outputTag;

    private HashMap<String, TableProcess> tableProcessMap;

    private HashSet<String> checkTable;

    private Connection connection;

    public DbSplitProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化
        tableProcessMap = new HashMap<>();

        checkTable = new HashSet<>();

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        refresMeta();

        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refresMeta();
            }
        },10000L,5000L);


    }

    private void refresMeta() {
        System.out.println("开始读取MySQL配置信息！");
        //查询MySQL
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
        //封装同步并检查与建表
        for (int i = 0; i < tableProcesses.size(); i++) {
            TableProcess tableProcess = tableProcesses.get(i);
            String key = tableProcess.getSourceTable() + ":"+ tableProcess.getOperateType();

            tableProcessMap.put(key, tableProcess);

            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                boolean notExist = checkTable.add(tableProcess.getSinkTable());

                if (notExist) {
                    creatTable(tableProcess.getSinkTable(),
                            tableProcess.getSinkColumns(),
                            tableProcess.getSinkPk(),
                            tableProcess.getSinkExtend());
                }
            }
        }
        if (tableProcessMap == null || tableProcessMap.size() == 0){
            throw new RuntimeException("读取MySQL配置信息失败！");
        }
    }

    private void creatTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        StringBuilder sql = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA).append(".").append(sinkTable).append("(");

        String[] column = sinkColumns.split(",");
        for (int i = 0; i < column.length; i++) {

            if (i == column.length -1){
                sql.append(column[i]).append(" varchar )");
            }else {
                if (sinkPk.equals(column[i])){
                    sql.append(column[i]).append(" varchar primary key");
                }else{
                    sql.append(column[i]).append(" varchar");
                }
                sql.append(",");
            }
        }
        sql.append(sinkExtend);

        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建Phoenix表" + sinkTable + "失败！");
        } finally {
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        out.collect(value);
    }
}
