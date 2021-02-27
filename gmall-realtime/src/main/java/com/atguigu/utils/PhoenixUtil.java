package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Aol
 * @create 2021-02-26 16:00
 */
public class PhoenixUtil {
    //声明连接
    private static Connection connection;

    private static Connection init(){
        try {
            //指定驱动、获取phoenix连接、指定库名
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            Connection phoenixConnection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            phoenixConnection.setSchema(GmallConfig.HBASE_SCHEMA);
            return phoenixConnection;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败!");
        }
    }


    public static <T>List<T> queryList(String sql ,Class<T>  cls){
        //判断连接是否为空、获取连接
        if (connection == null){
            connection = init();
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            //预编译sql
            preparedStatement = connection.prepareStatement(sql);
            //获取查询结果集
            resultSet = preparedStatement.executeQuery();
            //获取元数据
            ResultSetMetaData metaData = resultSet.getMetaData();
            //创建对应属性list
            ArrayList<T> list = new ArrayList<>();
            //获取字段数
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                //创建泛型对象
                T t = cls.newInstance();
                //为对象属性附值
                for (int i = 1; i <= columnCount; i++) {
                    BeanUtils.setProperty(t,metaData.getColumnName(i),resultSet.getObject(i));
                }
                //加入list
                list.add(t);
            }
            //返回list
            return list;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询维度信息失败！");
        } finally {
            //关闭结果集和预编译
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static void main(String[] args) {

        System.out.println(queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));

    }
}
