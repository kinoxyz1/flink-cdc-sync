package com.kino.flink.connector;

import org.apache.flink.api.connector.source.Source;

/**
 * @author kino
 * @date 2023/7/19 2:10 AM
 */
public interface BaseCDC {
    Source createBuild();
}
