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
 * ��HBaseGet.java��ʵ��������HBase Getʹ��Demo
 * 
 * �ܷ���ݿ�����Ա�ṩ��Object���Զ�ʵ���к�Object֮���ת�������⿪����Ա�Լ�ȥת��
 * </pre>
 * 
 * @author yangbolin Nov 8, 2013 1:36:18 PM
 */
public class HBaseGet {

    private static final Logger logger = LoggerFactory.getLogger(HBaseGet.class);

    /**
     * <pre>
     * �������RowKey��ѯһ�м�¼������һ�м�¼��Ӧ�ж���У���Щ�ж�����KV����ʽ����
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
                System.out.println("Family:" + Bytes.toString(kv.getFamily())); // ��������
                System.out.println("Qualifier: " + Bytes.toString(kv.getQualifier())); // ���������
                System.out.println("Key:" + Bytes.toString(kv.getRow())); // Key
                System.out.println("Value:" + Bytes.toString(kv.getValue())); // Value
            }
        } catch (IOException e) {
            logger.error(e);
        }
    }

    /**
     * ����RowKey����������ȡָ�������ݵ�Ԫ
     * 
     * @param rowKey
     * @param column
     */
    public static void doGetByRowKeyAndCol(String rowKey, String column) {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes("COL_NAME_A"));
        get.setMaxVersions();   //��ȡϵͳĿǰ���洢�����а汾�����ݵ�Ԫ��ϵͳĬ�ϴ洢�����汾�����ݵ�Ԫ�����ʹ�÷���ȥ�Լ�����Ҫ�洢���ٸ��汾�Ļ�
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
     * ����һ��Get���б�����ȡһ������
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
        // 0. ����ָ����RowKey����ȡ��¼
        doGetByRowKey("HBASE_PUT_ROW_KEY_A");
        // 1. ����ָ����RowKey����������ȡ���ݵ�Ԫ
        doGetByRowKeyAndCol("HBASE_PUT_ROW_KEY_B", "COL_NAME_B");
        // 2. ����һ��Get��list����ȡһ������
        doGetList();
    }
}
