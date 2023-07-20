package com.kino.flink.connector.mysql;

/**
 * @author kino
 * @date 2023/7/20 1:32 AM
 */
public enum DbzOp {
    c,
    r,
    u,
    d;

    public static final String COLUMN_KEY = "op";
    public static final String SOURCE_KEY = "source";
    public static final String SOURCE_DB_KEY = "db";
    public static final String SOURCE_TABLE_KEY = "table";
    public static final String BEFORE_KEY = "before";
    public static final String AFTER_KEY = "after";
}
