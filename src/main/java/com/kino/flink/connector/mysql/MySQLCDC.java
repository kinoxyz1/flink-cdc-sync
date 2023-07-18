package com.kino.flink.connector.mysql;

import com.kino.flink.connector.BaseCDC;
import org.apache.flink.api.connector.source.Source;

/**
 * @author kino
 * @date 2023/7/19 2:11 AM
 */
public class MySQLCDC implements BaseCDC {
    @Override
    public Source createBuild() {
        return null;
    }
}
