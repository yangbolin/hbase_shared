/*
 * Copyright 2013 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.alibaba.hbase.shared.demo.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

/**
 * <pre>
 * 类HBaseCommon.java的实现描述：负责初始化HBase的一些一些基本配置
 * 
 * 采用单例模式
 * </pre>
 * 
 * @author yangbolin Nov 6, 2013 4:07:53 PM
 */
public class HBaseCommon {

    private static Configuration configuration;
    private static HTablePool    pool;

    static {
        // 目前使用本地伪分布式的配置
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.client.retries.number", "1");
        configuration.set("hbase.client.operation.timeout", "2");
        //configuration.set("xx", "xx");
        pool = new HTablePool(configuration, HBaseSharedContants.HBASE_POOL_SIZE);
    }

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static HTableInterface getTable(String tableName) {
        try {
            return pool.getTable(tableName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return null;
        }
    }
}
