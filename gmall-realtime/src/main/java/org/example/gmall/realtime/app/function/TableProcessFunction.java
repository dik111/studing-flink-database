package org.example.gmall.realtime.app.function;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.gmall.realtime.bean.TableProcess;
import org.example.gmall.realtime.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Desription:
 *
 * @ClassName TableProcessFunction
 * @Author Zhanyuwei
 * @Date 2021/11/7 12:03 下午
 * @Version 1.0
 **/
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private Connection connection;

    //侧输出流标记属性
    private OutputTag<JSONObject> outputTag;

    //Map状态描述器属性
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {


    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

        // 1.获取并解析数据
        JSONObject jsonObject = JSON.parseObject(value);
        String data = jsonObject.getString("after");
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        // 2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),tableProcess.getSinkColumns(),tableProcess.getSinkPk(),tableProcess.getSinkExtend());
        }

        // 3.写入状态，广播出去
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);
    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if (sinkExtend == null){
            sinkExtend = "";
        }
        if (sinkPk == null){
            sinkPk = "id";
        }

        StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append(" (");

        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];

            // 判断是否为主键
            if (sinkPk.equals(field)){
                createTableSQL.append(field).append(" varchar primary key ");
            }else {
                createTableSQL.append(field).append(" varchar  ");
            }

            // 判断是否为最后一个字段，如果不是，则添加 逗号
            if (i < fields.length - 1){
                createTableSQL.append(",");
            }
        }
        createTableSQL.append(")").append(sinkExtend);

        System.out.println(createTableSQL);
    }
}
