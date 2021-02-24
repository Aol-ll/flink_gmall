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
public class DbSplitProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    private HashMap<String, TableProcess> tableProcessMap;
    private HashSet<String> existsTable;
    private Connection connection;

    public DbSplitProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //属性附值
        tableProcessMap = new HashMap<>();
        existsTable = new HashSet<>();
        //获取连接
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //初次执行
        refreshMeta();
        //开启周期调度
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        },10000L,5000L);
    }

    private void refreshMeta() {
        //查询MySQL获取配置信息
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
        //同步并检查与建表
        for (int i = 0; i < tableProcesses.size(); i++) {
            //获取sourceTable和operateType作为联key
            TableProcess tableProcess = tableProcesses.get(i);
            String sourceTable = tableProcess.getSourceTable();
            String operateType = tableProcess.getOperateType();
            String key = sourceTable + ":" + operateType;
            //放入map中等待读取配置信息
            tableProcessMap.put(key, tableProcess);
            //输出类型为hbase才建表,放入set判断其是否存在
            if ("insert".equals(operateType) && TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                boolean notExist = existsTable.add(tableProcess.getSinkTable());
                if (notExist) {
                    checkTable(
                            tableProcess.getSinkTable(),
                            tableProcess.getSinkColumns(),
                            tableProcess.getSinkPk(),
                            tableProcess.getSinkExtend()
                    );
                }
            }

        }
        if (tableProcessMap == null || tableProcessMap.size() == 0) {
            throw new RuntimeException("读取MySQL配置信息失败！");
        }
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        PreparedStatement preparedStatement = null;
        //判断并对主键和扩展语句加默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        try {
            //编写建表sql
            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".").append(sinkTable).append("(");
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (i == columns.length - 1) {
                    sql.append(column).append(" varchar)");
                } else {
                    if (sinkPk.equals(column)) {
                        sql.append(column).append(" varchar primary key");
                    } else {
                        sql.append(column).append(" varchar");
                    }
                    sql.append(",");
                }
            }
            sql.append(sinkExtend);
            //获取预编译sql并执行
            preparedStatement = connection.prepareStatement(sql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
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
        //读取tableProcessMap中的表信息
        String table = value.getString("table");
        String type = value.getString("type");
        //Maxwell的返回值类型为"bootstrap-insert
        if ("bootstrap-insert".equals(type)) {
            value.put("type", "insert");
        }
        String key = table + ":" + type;

        TableProcess tableProcess = tableProcessMap.get(key);

        if (tableProcess != null) {
            value.put("sink_table", tableProcess.getSinkTable());
            //按照指定字段 进行过滤
            folterColumn(tableProcess.getSinkColumns(), value.getJSONObject("data"));
            //按照其输出类型进行分流
            if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
                ctx.output(outputTag, value);
            } else if (TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())) {
                out.collect(value);
            }
        } else {
            System.out.println("No Key " + key + " In Mysql!");
        }
    }

    private void folterColumn(String sinkColumns, JSONObject data) {
        String[] fileds = sinkColumns.split(",");
        List<String> list = Arrays.asList(fileds);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !list.contains(next.getKey()));
    }
}
