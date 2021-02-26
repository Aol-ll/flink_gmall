package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author Aol
 * @create 2021-02-26 16:01
 * select * from t where id='19' and name='liuao'
 */
public class DimUtil {
    public static JSONObject getDimInfo(String tablename, Tuple2<String, String>... columnValues) {
        if (columnValues.length <= 0) {
            throw new RuntimeException("查询维度数据时,请至少设置一个查询条件！");
        }
        StringBuilder sql = new StringBuilder("select * from ").append(tablename).append(" where ");
        StringBuilder key = new StringBuilder(tablename).append(":");
        for (int i = 0; i < columnValues.length; i++) {
            Tuple2<String, String> columnValue = columnValues[i];
            String column = columnValue.f0;
            String value = columnValue.f1;
            sql.append(column).append("='").append(value).append("'");
            key.append(value);
            if (i < columnValues.length - 1) {
                sql.append(" and ");
                key.append(":");
            }
        }
        System.out.println(sql);

        Jedis jedis = RedisUtil.getJedis();
        String redisValue = jedis.get(key.toString());
        if (redisValue != null && redisValue.length() > 0) {
            jedis.close();
            return JSON.parseObject(redisValue);
        }
        List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql.toString(), JSONObject.class);
        JSONObject jsonObject = jsonObjects.get(0);

        jedis.set(key.toString(), jsonObject.toString());
        jedis.expire(key.toString(), 24 * 60 * 60);
        jedis.close();
        return jsonObject;
    }


    public static JSONObject getDimInfo(String tableName, String value) {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }

    public static void deleteCached(String tableName,String id) {
        String key = tableName.toUpperCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(getDimInfo("DIM_BASE_TRADEMARK", "23"));
    }
}
