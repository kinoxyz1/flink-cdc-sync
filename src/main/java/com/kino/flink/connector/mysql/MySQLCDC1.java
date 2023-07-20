package com.kino.flink.connector.mysql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.row.sink.StarRocksSinkOP;
import com.starrocks.connector.flink.table.data.DefaultStarRocksRowData;
import com.starrocks.connector.flink.table.sink.StarRocksDynamicSinkFunction;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.sink.StarRocksSinkRowDataWithMeta;
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
 * 不分流的做法, 弊端如下:
 *   1. 在 flink-ui 上看是的 records 是同步所有库的记录总数, 无法看见每张表的数据量;
 *   2. starrocks2.4之后支持用主键更新部分列，不分流的话，只能做到全字段更新不能部分更新.
 * @author kino
 * @date 2023/7/19 2:11 AM
 */
public class MySQLCDC1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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

        env.enableCheckpointing(10000);
        DataStreamSource<String> fromSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        SingleOutputStreamOperator<StarRocksSinkRowDataWithMeta> mapStream = fromSource.map(new MapFunction<String, StarRocksSinkRowDataWithMeta>() {
            @Override
            public StarRocksSinkRowDataWithMeta map(String s) {
                JSONObject jsonRecord = JSON.parseObject(s);
                StarRocksSinkRowDataWithMeta rowDataWithMeta = new StarRocksSinkRowDataWithMeta();
                rowDataWithMeta.setTable(JSON.parseObject(jsonRecord.getString("source")).getString("table"));
                rowDataWithMeta.setDatabase("kinodb");
                rowDataWithMeta.addDataRow(JSON.parseObject(jsonRecord.getString("after")).toString());

                String op = jsonRecord.getString(DbzOp.COLUMN_KEY);
                if (DbzOp.c.name().equals(op) || DbzOp.r.name().equals(op) || DbzOp.u.name().equals(op)) {
                    // rowDataWithMeta.setRow(jsonRecord.getJSONObject("after").toJSONString());
                }  else if (DbzOp.d.name().equals(op)) {
                    // rowDataWithMeta.setRow(jsonRecord.getJSONObject("before").toJSONString());
                }
                return rowDataWithMeta;
            }
        }).startNewChain();

        StarRocksSinkOptions build = StarRocksSinkOptions.builder()
                .withProperty("jdbc-url", "jdbc:mysql://127.0.0.1:9930")
                .withProperty("load-url", "127.0.0.1:9030")
                .withProperty("username", "root")
                .withProperty("password", "000000")
                .withProperty("database-name", "kinodb")
                .withProperty("table-name", "")
                // .withProperty("sink.properties.partial_update", "true")
                // .withProperty("sink.properties.columns", "id,name,description,__op")
                .withProperty("sink.properties.format", "json")
                .withProperty("sink.properties.strip_outer_array", "true")
                .withProperty("sink.parallelism", "1")
                .build();

        mapStream.addSink(new StarRocksDynamicSinkFunction<>(build));


        env.execute("Print MySQL Snapshot + Binlog");
    }
}
