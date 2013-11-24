/*
 * Copyright 2013 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.alibaba.hbase.shared.demo.scan;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.common.logging.Logger;
import com.alibaba.common.logging.LoggerFactory;
import com.alibaba.hbase.shared.demo.common.HBaseCommon;
import com.alibaba.hbase.shared.demo.common.HBaseSharedContants;

/**
 * <pre>
 * 类HBaseScan.java的实现描述：HBase Scan的相关操作
 * 
 * 使用HBase的scan时，记得在最后进行close操作
 * </pre>
 * @author yangbolin Nov 6, 2013 7:35:37 PM
 */
public class HBaseScan {
    private static final Logger logger = LoggerFactory.getLogger(HBaseScan.class);
    /**
     * 全表扫描
     */
    public static void doScanAll() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        // 在构造Scan实例的时候，要是不指定任何东西的话，则会进行全表扫描
        Scan scan = new Scan();
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                for (KeyValue kv : result.raw()) {
                    System.out.println(kv);
                }
            }
            resultScanner.close();  // Close scanner to free remote resources
        } catch (Exception e) {
            logger.error(e);
        }
    }
    
    /**
     * 扫描某一列族下面的所有数据
     */
    public static void doScanFamily() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME));
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                for (KeyValue kv : result.raw()) {
                    System.out.println(kv);
                }
            }
            resultScanner.close();  // Close scanner to free remote resources
        }catch (Exception e) {
            logger.error(e);
        }
    }
    
    /**
     * 扫描某一列下面的所有数据
     */
    public static void doScanColumn() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes("COL_NAME_A"));
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                for (KeyValue kv : result.raw()) {
                    System.out.println(kv);
                }
            }
            resultScanner.close();  // Close scanner to free remote resources
        } catch (Exception e) {
            logger.error(e);
        }
    }
    
    /**
     * <pre>
     * 扫描一个rowKey区间里的数据
     * 注意：
     * 这个rowKey的区间也是一个左闭右开的区间，[startRowKey, endRowKey)
     * </pre>
     * @param startRowKey
     * @param endRowKey
     */
    public static void doScanSection(String startRowKey, String endRowKey) {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRowKey));
        scan.setStopRow(Bytes.toBytes(endRowKey));
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            for(Result result : resultScanner) {
                for(KeyValue kv : result.raw()) {
                    System.out.println(kv);
                }
            }
            resultScanner.close();  // Close scanner to free remote resources
        } catch (Exception e) {
            logger.error(e);
        }
    }
    
    public static void main(String[] args) {
        // 0. 全表扫描
        System.out.println("scan all data from table start...");
        doScanAll();
        System.out.println("scan all data from table end...");
        // 1. 扫描某一列族下面的所有数据
        System.out.println("scan all data from column family start...");
        doScanFamily();
        System.out.println("scan all data from column family end...");
        // 2. 扫描某一列族中某一列下面的所有数据
        System.out.println("scan all data from column start...");
        doScanColumn();
        System.out.println("scan all data from column end...");
        // 3. 扫描一个rowkey的区间
        System.out.println("scan all data from rowkey section start...");
        doScanSection("HBASE_PUT_ROW_KEY_A", "HBASE_PUT_ROW_KEY_B");
        System.out.println("scan all data from rowkey section end...");
    }
}
