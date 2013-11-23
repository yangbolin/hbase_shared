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

import com.alibaba.common.logging.Logger;
import com.alibaba.common.logging.LoggerFactory;
import com.alibaba.hbase.shared.demo.common.HBaseCommon;
import com.alibaba.hbase.shared.demo.common.HBaseSharedContants;

/**
 * ��HBaseScan.java��ʵ��������HBase Scan����ز���
 * @author yangbolin Nov 6, 2013 7:35:37 PM
 */
public class HBaseScan {
    private static final Logger logger = LoggerFactory.getLogger(HBaseScan.class);
    /**
     * ȫ��ɨ��
     */
    public static void doScanAll() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        // �ڹ���Scanʵ����ʱ��Ҫ�ǲ�ָ���κζ����Ļ���������ȫ��ɨ��
        Scan scan = new Scan();
        try {
            ResultScanner resultScanner = hTable.getScanner(scan);
            for (Result result : resultScanner) {
                for (KeyValue kv : result.raw()) {
                    System.out.println(kv);
                }
            }
        } catch (Exception e) {
            logger.equals(e);
        }
    }
    
    public static void main(String[] args) {
        // 0.ȫ��ɨ��
        doScanAll();
    }
}
