package cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desription:
 *
 * @ClassName FlinkCDC
 * @Author Zhanyuwei
 * @Date 2021/10/31 12:56
 * @Version 1.0
 **/
public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        // 获取执行环节
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 通过flink cdc构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("pigx-mysql")
                .port(3306)
                .username("root")
                .password("asd2828")
                .databaseList("test_flink")
                .tableList("test_flink.base_trademark")
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);

        // 打印数据
        dataStreamSource.print();

        // 启动任务
        env.execute("FlinkCDC");
    }
}
