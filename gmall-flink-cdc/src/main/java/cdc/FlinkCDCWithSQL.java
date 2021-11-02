package cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Desription:
 *
 * @ClassName FlinkCDCWithSQL
 * @Author Zhanyuwei
 * @Date 2021/10/31 17:37
 * @Version 1.0
 **/
public class FlinkCDCWithSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE mysql_binlog ( " +
                " id STRING NOT NULL, " +
                " tm_name STRING, " +
                " logo_url STRING " +
                ") WITH ( " +
                " 'connector' = 'mysql-cdc', " +
                " 'hostname' = 'pigx-mysql', " +
                " 'port' = '3306', " +
                " 'username' = 'root', " +
                " 'password' = '000000', " +
                " 'database-name' = 'flink_test', " +
                " 'table-name' = 'base_trademark' " +
                ")");

        Table table = tableEnv.sqlQuery("select * from mysql_binlog");

        DataStream<Tuple2<Boolean, Row>> retractStream  = tableEnv.toRetractStream(table, Row.class);

        retractStream.print();

        //5.启动任务
        env.execute("FlinkCDCWithSQL");
    }
}
