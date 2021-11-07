package org.example.gmall.realtime.common;

/**
 * Desription:
 *
 * @ClassName GmallConfig
 * @Author Zhanyuwei
 * @Date 2021/11/7 10:02 下午
 * @Version 1.0
 **/
public class GmallConfig {


    //Phoenix 库名
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";
    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:node2.ambari.com,node1.ambari.com,node3.ambari.com:2181";
}
