package com.atguigu.app.func;


import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import com.atguigu.utils.RedisUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author Aol
 * @create 2021-02-23 16:11
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            //获取表信息
            JSONObject data = value.getJSONObject("data");
            String table = value.getString("sink_table");
            Set<String> keySet = data.keySet();
            Collection<Object> values = data.values();
            //生成sql语句
            String sql = getUpersertSql(table, keySet, values);
            System.out.println(sql);
            //预编译sql
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
            connection.commit();
            //判断操作类型、对update的数据对应的redis中的缓存进行删除
            String type = value.getString("type");
            if ("update".equals(type)) {
                DimUtil.deleteCached(table,data.getString("id"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String getUpersertSql(String table, Set<String> keySet, Collection<Object> values) {

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "."
                + table  + "(" + StringUtils.join(keySet,",") + ")"
                + " values" + "('"+StringUtils.join(values,"','")+"')";
    }
}
