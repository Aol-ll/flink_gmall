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
        if (connection == null){
            connection = init();
        }

        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(sql);

            resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();

            ArrayList<T> list = new ArrayList<>();


            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                T t = cls.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    BeanUtils.setProperty(t,metaData.getColumnName(i),resultSet.getObject(i));

                }
                list.add(t);
            }
            return list;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("查询维度信息失败！");
        } finally {
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
