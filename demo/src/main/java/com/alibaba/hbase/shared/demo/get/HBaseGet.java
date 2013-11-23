/*
 * Copyright 2013 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.alibaba.hbase.shared.demo.get;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.common.logging.Logger;
import com.alibaba.common.logging.LoggerFactory;
import com.alibaba.hbase.shared.demo.common.HBaseCommon;
import com.alibaba.hbase.shared.demo.common.HBaseSharedContants;

/**
 * <pre>
 * 类HBaseGet.java的实现描述：HBase Get使用Demo
 * 
 * 能否根据开发人员提供的Object，自动实现列和Object之间的转换，避免开发人员自己去转换
 * </pre>
 * 
 * @author yangbolin Nov 8, 2013 1:36:18 PM
 */
public class HBaseGet {

    private static final Logger logger = LoggerFactory.getLogger(HBaseGet.class);

    /**
     * <pre>
     * 这里根据RowKey查询一行记录出来，一行记录对应有多个列，这些列都会以KV的形式返回
     * </pre>
     * 
     * @param rowKey
     */
    public static void doGetByRowKey(String rowKey) {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Get get = new Get(Bytes.toBytes(rowKey));
        
        get.addFamily(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME));

        try {
            Result result = hTable.get(get);
            List<KeyValue> kvs = result.list();
            for (KeyValue kv : kvs) {
                System.out.println("Family:" + Bytes.toString(kv.getFamily())); // 列族的输出
                System.out.println("Qualifier: " + Bytes.toString(kv.getQualifier())); // 列名的输出
                System.out.println("Key:" + Bytes.toString(kv.getRow())); // Key
                System.out.println("Value:" + Bytes.toString(kv.getValue())); // Value
            }
        } catch (IOException e) {
            logger.error(e);
        }
    }

    /**
     * 根据RowKey和列名来获取指定的数据单元
     * 
     * @param rowKey
     * @param column
     */
    public static void doGetByRowKeyAndCol(String rowKey, String column) {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes("COL_NAME_A"));
        get.setMaxVersions();   //获取系统目前所存储的所有版本的数据单元，系统默认存储三个版本的数据单元，如果使用方不去自己设置要存储多少个版本的话
        try {
            Result result = hTable.get(get);
            List<KeyValue> kvs = result.list();
            for (KeyValue kv : kvs) {
                System.out.println("Version:" + kv.getTimestamp());
                System.out.println("Family:" + Bytes.toString(kv.getFamily()));
                System.out.println("Qualifier:" + Bytes.toString(kv.getQualifier()));
                System.out.println("Key:" + Bytes.toString(kv.getRow()));
                System.out.println("Value: " + Bytes.toString(kv.getValue()));
            }
        } catch (IOException e) {
            logger.error(e);
        }
    }
    
    /**
     * 根据一个Get的列表来获取一批数据
     */
    public static void doGetList() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        List<Get> getList = new ArrayList<Get>();
        Get getA = new Get(Bytes.toBytes("HBASE_PUT_ROW_KEY_A"));
        getList.add(getA);
        Get getB = new Get(Bytes.toBytes("HBASE_PUT_ROW_KEY_B"));
        getList.add(getB);
        
        try {
            Result[] resultArray = hTable.get(getList);
            if (resultArray == null || resultArray.length == 0) {
                return;
            }
            
            System.out.println("###############doGetList###########################");
            for (int i = 0; i < resultArray.length; ++i) {
                List<KeyValue> kvs = resultArray[i].list();
                for (KeyValue kv : kvs) {
                    System.out.println("Version: " + kv.getTimestamp());
                    System.out.println("Family: " + Bytes.toString(kv.getFamily()));
                    System.out.println("Qualifier: " + Bytes.toShort(kv.getQualifier()));
                    System.out.println("Key: " + Bytes.toString(kv.getKey()));
                    System.out.println("Value: " + Bytes.toString(kv.getValue()));
                }
            }
            System.out.println("###################################################");
        } catch (Exception e) {
            logger.error(e);
        }
    }
    
    public static void main(String[] args) {
        // 0. 根据指定的RowKey来获取记录
        doGetByRowKey("HBASE_PUT_ROW_KEY_A");
        // 1. 根据指定的RowKey和列名来获取数据单元
        doGetByRowKeyAndCol("HBASE_PUT_ROW_KEY_B", "COL_NAME_B");
        // 2. 根据一个Get的list来获取一批数据
        doGetList();
    }
}
