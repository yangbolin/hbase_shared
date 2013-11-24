/*
 * Copyright 2013 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.alibaba.hbase.shared.demo.filter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.common.logging.Logger;
import com.alibaba.common.logging.LoggerFactory;
import com.alibaba.hbase.shared.demo.common.HBaseCommon;
import com.alibaba.hbase.shared.demo.common.HBaseSharedContants;

/**
 * ��HBaseFilter.java��ʵ��������Filterd�Ļ����÷�
 * 
 * @author yangbolin Nov 24, 2013 4:18:29 PM
 */
public class HBaseFilter {

    private static final Logger logger = LoggerFactory.getLogger(HBaseFilter.class);

    /**
     * ����rowkey��Filter
     */
    public static void doRowKeyFilter() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        /** ɨ���rowkeyС�ڵ���HBASE_PUT_ROW_KEY_D���������ݵ�Ԫ **/
        Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                                      new BinaryComparator(Bytes.toBytes("HBASE_PUT_ROW_KEY_D")));
        Scan scan = new Scan();
        scan.setFilter(filter);
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                for (KeyValue kv : result.raw()) {
                    System.out.println(kv);
                }
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * ��ҳ��ѯ�Ĺ�����
     */
    public static void doPageFilter() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Filter pageFilter = new PageFilter(3);  // ҳ��С��3
        Scan scan = new Scan();
        scan.setFilter(pageFilter);
        
        // ��һҳ���һ��rowkey�ĳ�ʼ��
        byte[] lastRow = null;
        // ����������
        int totalRows = 0;
        
        while(true) {
            if(lastRow != null) {
                lastRow = Bytes.add(lastRow, Bytes.toBytes(1));
                System.out.println("start row: " + Bytes.toString(lastRow));
                scan.setStartRow(lastRow);
            }
            
            ResultScanner resultScanner = null;
            try {
                resultScanner = hTable.getScanner(scan);
                int localRows = 0;
                Result result = null;
                while ((result = resultScanner.next()) != null) {
                    System.out.println(localRows++  + " : " + result);
                    lastRow = result.getRow();
                }
                totalRows += localRows;
                
                resultScanner.close();
                if (localRows == 0) {
                    System.out.println(String.format("total rows = %d", totalRows));
                    break;
                }
            } catch (Exception e) {
                logger.error(e);
                break;
            }
        }
    }
    public static void main(String[] args) {
        // 0. ����rowkey��filter
        System.out.println("based on rowkey filter start....");
        doRowKeyFilter();
        System.out.println("based on rowkey filter end...");
        // 1. ��ҳ��ѯ
        System.out.println("page filter start...");
        doPageFilter();
        System.out.println("page filter end...");
    }
}