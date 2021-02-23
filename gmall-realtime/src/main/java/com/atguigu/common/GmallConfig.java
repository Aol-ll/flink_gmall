package com.atguigu.common;

/**
 * @author Aol
 * @create 2021-02-23 11:06
 */
public class GmallConfig {
    //hbase命名空间
    public static final String HBASE_SCHEMA="GMALL200821_REALTIME";
    //phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //phoenix连接
    public static final String PHOENIX_SERVER="jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

}
