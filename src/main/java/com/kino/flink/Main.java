package com.kino.flink;

import com.kino.flink.connector.BaseCDC;
import com.kino.flink.connector.mysql.MySQLCDC;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author kino
 * @date 2023/7/19 2:15 AM
 */
public class Main {
    public static void main(String[] args) {
        // TODO: 解析 sql, 根据不同的数据库创建对应的cdc对象
        BaseCDC cdc = new MySQLCDC();
        Source build = cdc.createBuild();
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        SingleOutputStreamOperator operator =
                localEnvironment.fromSource(
                        build,
                        WatermarkStrategy.noWatermarks(),
                        ""
                        ).startNewChain();
    }
}
