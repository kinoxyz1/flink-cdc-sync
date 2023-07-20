package com.kino.flink.connector.mysql;

import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author kino
 * @date 2023/7/19 2:11 AM
 */
public class MySQLCDC {
    public static void main(String[] args) throws Exception {
        HashMap<String,String> tables = new HashMap<>();
        tables.put("mydb.orders", "order_date,order_status,price,product_id,customer_name,order_id,__op");
        tables.put("mydb.products", "id,name,description,__op");

        Properties prop = new Properties();
        prop.setProperty("time_zone", "+8:00");
        prop.setProperty("serverTimeZone", "Asia/Shanghai");
        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname("localhost")
                .port(3306)
                .scanNewlyAddedTableEnabled(true) // 启用扫描新添加的表功能
                .databaseList("mydb")
                .tableList("mydb.*")
                .username("root")
                .password("123456")
                .jdbcProperties(prop)
                .deserializer(new JsonDebeziumDeserializationSchema())  // 将 SourceRecord 转换为 JSON 字符串
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.enableCheckpointing(10000);
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(), "MySQL Source");

        SingleOutputStreamOperator<Object> processStream = mySQLSource.process(new ProcessFunction<String, Object>() {
            private static final long serialVersionUID = 6824863705622927587L;
            @Override
            public void processElement(
                    String data,
                    ProcessFunction<String, Object>.Context context,
                    Collector<Object> collector) {
                JSONObject dataJson = JSONObject.parseObject(data);
                JSONObject source = dataJson.getJSONObject("source");
                JSONObject after = dataJson.getJSONObject("after");
                String dbName = source.getString("db");
                String tableName = source.getString("table");
                OutputTag<String> outputTag = new OutputTag<String>(dbName + "." + tableName) {};
                context.output(outputTag, data);
            }
        }).startNewChain();

        for (Map.Entry<String, String> table : tables.entrySet()) {
            OutputTag<String> ot = new OutputTag<String>(table.getKey()){};
            DataStream<String> sideOutput = processStream.getSideOutput(ot);
            SingleOutputStreamOperator<String> sinkStream = sideOutput.map(new MapFunction<String, String>() {
                @Override
                public String map(String s) {
                    System.out.println(s);
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    String op = jsonObject.getString(DbzOp.COLUMN_KEY);
                    JSONObject data = null;
                    if (DbzOp.c.name().equals(op) || DbzOp.r.name().equals(op) || DbzOp.u.name().equals(op)) {
                        data = jsonObject.getJSONObject("after");
                        data.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.UPSERT.ordinal());
                    } else if (DbzOp.d.name().equals(op)) {
                        data = jsonObject.getJSONObject("before");
                        data.put(StarRocksSinkOP.COLUMN_KEY, StarRocksSinkOP.DELETE.ordinal());
                    }
                    return data.toString();
                }
            }).startNewChain();

            sinkStream.addSink(StarRocksSink.sink(
                    StarRocksSinkOptions.builder()
                            .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9930")
                            .withProperty("load-url", "127.0.0.1:9030")
                            .withProperty("username", "root")
                            .withProperty("password", "000000")
                            .withProperty("database-name", "kinodb")
                            .withProperty("table-name", table.getKey().split("\\.")[1])
                            .withProperty("sink.properties.partial_update", "true")
                            .withProperty("sink.properties.columns", table.getValue())
                            .withProperty("sink.properties.format", "json")
                            .withProperty("sink.properties.strip_outer_array", "true")
                            .withProperty("sink.parallelism", "1")
                            .build()
            )).name("StarRocks["+table.getKey()+"]");
        }


        env.execute("Print MySQL Snapshot + Binlog");
    }
}
