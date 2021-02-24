package com.atguigu.utils;



import com.atguigu.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Aol
 * @create 2021-02-22 11:43
 */
public class MySQLUtil {


    public static <T> List<T> queryList(String sql, Class<T> cls, boolean underScoreToCamel) {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            //注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //获取连接
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/gmall_realtime_2021?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");

            //预编译sql
            preparedStatement = connection.prepareStatement(sql);
            //获取查询结果
            resultSet = preparedStatement.executeQuery();
            //创建返回值数组
            ArrayList<T> list = new ArrayList<>();
            //获取原数据信息
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            //向list中添加元素
            while (resultSet.next()){
                //创建元素对象
                T t = cls.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    //获取原数据
                    String columnName = metaData.getColumnName(i);
                    if (underScoreToCamel){
                        //将"_"转化为小驼峰
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //将字段数据填入对象中
                    BeanUtils.setProperty(t,columnName,resultSet.getObject(i));
                }
                //集合中添加元素
                list.add(t);
            }
            //返回list
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("没有获得连接");
        } finally {
            //释放资源
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
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
