package org.example.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.gmall.realtime.app.function.CustomerDeserialization;
import org.example.gmall.realtime.app.function.TableProcessFunction;
import org.example.gmall.realtime.bean.TableProcess;
import org.example.gmall.realtime.utils.MyKafkaUtil;
import scala.tools.nsc.doc.model.Val;

/**
 * Desription:
 *
 * @ClassName BaseDBApp
 * @Author Zhanyuwei
 * @Date 2021/11/6 5:37 下午
 * @Version 1.0
 **/
public class BaseDBApp {

    public static void main(String[] args) {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 设置CK&状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink-210325/ck"));
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);

        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart());


        // 2.消费Kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_log_app_20211106";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        // 3.将每行数据转换成JSON对象并过滤（delete）
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        String type = value.getString("type");
                        return !"delete".equals(type);
                    }
                });

        // 4.使用FlinkCDC消费配置表，并处理成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("pigx-mysql")
                .port(3306)
                .username("root")
                .password("asd2828")
                .databaseList("test_flink_2")
                .tableList("test_flink_2.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();
        DataStreamSource<String> tableProcessStrDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        // 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);

        // 6.处理数据 广播流数据，主流数据（根据广播流数据进行处理）
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        connectedStream.process(new TableProcessFunction(hbaseTag,mapStateDescriptor));

        // 7.提取kafka流数据和HBase流数据

        // 8.讲kafka数据数据写入kafka主题，将HBase数据写入phonenix表

        // 9.启动任务
    }
}
