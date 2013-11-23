/*
 * Copyright 2013 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.alibaba.hbase.shared.demo.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.common.logging.Logger;
import com.alibaba.common.logging.LoggerFactory;
import com.alibaba.hbase.shared.demo.common.HBaseCommon;
import com.alibaba.hbase.shared.demo.common.HBaseSharedContants;

/**
 * ��HBasePut.java��ʵ��������HBase���ݲ����Demo
 * 
 * @author yangbolin Nov 6, 2013 4:17:37 PM
 */
public class HBasePut {

    private static final Logger logger     = LoggerFactory.getLogger(HBasePut.class);
    private static final String ROW_KEY_A  = "HBASE_PUT_ROW_KEY_A";
    private static final String ROW_KEY_B  = "HBASE_PUT_ROW_KEY_B";
    private static final String ROW_KEY_C  = "HBASE_PUT_ROW_KEY_C";
    private static final String ROW_KEY_D  = "HBASE_PUT_ROW_KEY_D";
    private static final String COL_NAME_A = "COL_NAME_A";
    private static final String COL_NAME_B = "COL_NAME_B";
    private static final String COL_NAME_C = "COL_NAME_C";
    private static final String COL_NAME_D = "COL_NAME_D";

    /** �쳣������� **/
    private static final String ROW_KEY_E  = "HBASE_PUT_ROW_KEY_E";
    private static final String ROW_KEY_F  = "HBASE_PUT_ROW_KEY_F";

    /**
     * ��һ��������
     */
    public static void doPutSingleRow() {

        // ��HTbalePool�л�ȡһ��HTable��ʵ�������ƹ�ϵ�����ݿ��л�ȡ���ݿ������
        HTableInterface table = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);

        // ����RowKeyΪHBASE_PUT_ROW_KEY_A��Putʵ����һ��Putʵ�������һ��RowKey���
        Put put = new Put(Bytes.toBytes(ROW_KEY_A));
        // ��RowKey=HBASE_PUT_ROW_KEY_A����һ������һ��COL_NAME_A=HBASE_PUT_COL_A
        put.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                Bytes.toBytes("HBASE_PUT_COL_A"));
        // ��RowKey=HBASE_PUT_ROW_KEY_A����һ������һ��COL_NAME_B=HBASE_PUT_COL_B
        put.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_B),
                Bytes.toBytes("HBASE_PUT_COL_B"));
        try {
            // ���ݵĲ���
            table.put(put);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * ���Ӷ�������
     */
    public static void doPutMulRows() {
        HTableInterface table = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);

        // �������ʵ��
        List<Put> putList = new ArrayList<Put>();

        Put putA = new Put(Bytes.toBytes(ROW_KEY_A));
        putA.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                 Bytes.toBytes("HBASE_PUT_COL_A"));
        putA.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_B),
                 Bytes.toBytes("HBASE_PUT_COL_B"));
        putA.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_C),
                 Bytes.toBytes("HBASE_PUT_COL_C"));
        putA.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_D),
                 Bytes.toBytes("HBASE_PUT_COL_D"));
        putList.add(putA);

        Put putB = new Put(Bytes.toBytes(ROW_KEY_B));
        putB.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                 Bytes.toBytes("HBASE_PUT_COL_A"));
        putB.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_D),
                 Bytes.toBytes("HBASE_PUT_COL_D"));
        putList.add(putB);

        Put putC = new Put(Bytes.toBytes(ROW_KEY_C));
        putC.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_B),
                 Bytes.toBytes("HBASE_PUT_COL_B"));
        putC.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_C),
                 Bytes.toBytes("HBASE_PUT_COL_C"));
        putList.add(putC);

        Put putD = new Put(Bytes.toBytes(ROW_KEY_D));
        putD.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_B),
                 Bytes.toBytes("HBASE_PUT_COL_B"));
        putD.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_C),
                 Bytes.toBytes("HBASE_PUT_COL_C"));
        putList.add(putD);

        try {
            table.put(putList);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * <pre>
     * ���ж��в����ʱ������һ�г��������쳣�����ᵼ�������������붼ʧ��
     * 
     * ע��
     * ������쳣���뱣֤Put�������ģ������壬��������ֵ������Ԫ�ز���ȱ�٣������ǻ�Ӱ�������Ĳ���
     * </pre>
     */
    public static void doPutMulRowsEx() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);

        List<Put> putList = new ArrayList<Put>();

        Put putE = new Put(Bytes.toBytes(ROW_KEY_E));
        putE.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                 Bytes.toBytes("HBASE_PUT_COL_A_FOR_E"));
        putE.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_B),
                 Bytes.toBytes("HBASE_PUT_COL_B_FOR_E"));
        putList.add(putE);

        Put putF = new Put(Bytes.toBytes(ROW_KEY_F));
        putF.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_C),
                 Bytes.toBytes("HBASE_PUT_COL_C_FOR_F"));
        putF.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_D),
                 Bytes.toBytes("HBASE_PUT_COL_D_FOR_F"));
        putList.add(putE);

        /**
         * <pre>
         * ����һ��Put��ʹ��һ�������ڵ����壬���Put�����ݲ���ע����ʧ�ܣ��쳣�׳� 
         * Column family XXXXX does not exist in region company_compare *
         * </pre>
         */
        Put putG = new Put(Bytes.toBytes(ROW_KEY_F));
        putG.add(Bytes.toBytes("XXXXX"), Bytes.toBytes(COL_NAME_A), Bytes.toBytes("HBASE_PUT_COL_A_FOR_G"));
        putList.add(putG);

        try {
            hTable.put(putList);
        } catch (IOException e) {
            logger.error(e);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * ���в��룬����ʹ����һ����������Put�����������Put�Ĳ���ʧ�ܻᵼ������Put�Ĳ���Ҳʧ��
     */
    public static void doPutMulRowsExEx() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);

        List<Put> putList = new ArrayList<Put>();

        Put putE = new Put(Bytes.toBytes(ROW_KEY_E));
        putE.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                 Bytes.toBytes("HBASE_PUT_COL_A_FOR_EE"));
        putE.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_B),
                 Bytes.toBytes("HBASE_PUT_COL_B_FOR_EE"));
        putList.add(putE);

        Put putF = new Put(Bytes.toBytes(ROW_KEY_F));
        putF.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_C),
                 Bytes.toBytes("HBASE_PUT_COL_C_FOR_FF"));
        putF.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_D),
                 Bytes.toBytes("HBASE_PUT_COL_D_FOR_FF"));
        putList.add(putE);

        /*
         * ����һ��Put������û��ָ�����壬���Լ��е�ֵ��������Ӱ�����������ݵĲ���
         */
        Put putG = new Put(Bytes.toBytes(ROW_KEY_F));
        // putG.add(Bytes.toBytes("XXXXX"), Bytes.toBytes(COL_NAME_A), Bytes.toBytes("HBASE_PUT_COL_A_FOR_G"));
        putList.add(putG);

        try {
            hTable.put(putList);
        } catch (IOException e) {
            logger.error(e);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * �������������ٸ���
     */
    public static void doCheckAndPut() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Put putB = new Put(Bytes.toBytes(ROW_KEY_B));
        putB.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                 Bytes.toBytes("HBASE_CHECK_AND_PUT_A"));

        try {
            /** ����COL_NAME_A��ֵΪNULL��ʱ�������A��ֵ����ʱ��COL_NAME_A��ֵ�϶�����NULL,��˷���ֵһ����false **/
            boolean res = hTable.checkAndPut(Bytes.toBytes(ROW_KEY_B),
                                             Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME),
                                             Bytes.toBytes(COL_NAME_A), null, putB);
            System.out.println(res);

            /** ����COL_NAME_C��ֵΪNULL��ʱ�������A��ֵ����ʱ��COL_NAME_C��ֵΪNULL,��˷���ֵһ����true **/
            res = hTable.checkAndPut(Bytes.toBytes(ROW_KEY_B), Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME),
                                     Bytes.toBytes(COL_NAME_C), null, putB);
            System.out.println(res);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public static void main(String[] args) {
        // 0. ��һ�����Ӷ���У�����ֱ����HBASE��shell�ͻ���ʹ�� scan 'compnay_compare' ���鿴����������
        doPutSingleRow();
        // 1. ���Ӷ���
        doPutMulRows();
        // 2. ���Ӷ��У�����һ�з����쳣�����ᵼ�������е�����Ҳʧ��
        doPutMulRowsEx();
        // 3. ���Ӷ��У���������һ����һ����������Put����ʱ����Ϊ�����������Put���������еĲ���Ҳʧ��
        doPutMulRowsExEx();
        // 4. �������ĳһ��������ִ�и���
        doCheckAndPut();
    }
}
