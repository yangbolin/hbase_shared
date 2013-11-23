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
 * 类HBasePut.java的实现描述：HBase数据插入的Demo
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

    /** 异常插入测试 **/
    private static final String ROW_KEY_E  = "HBASE_PUT_ROW_KEY_E";
    private static final String ROW_KEY_F  = "HBASE_PUT_ROW_KEY_F";

    /**
     * 给一行增多列
     */
    public static void doPutSingleRow() {

        // 从HTbalePool中获取一个HTable的实例，类似关系型数据库中获取数据库的链接
        HTableInterface table = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);

        // 构造RowKey为HBASE_PUT_ROW_KEY_A的Put实例，一个Put实例必须和一个RowKey相关
        Put put = new Put(Bytes.toBytes(ROW_KEY_A));
        // 对RowKey=HBASE_PUT_ROW_KEY_A的这一行增加一列COL_NAME_A=HBASE_PUT_COL_A
        put.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                Bytes.toBytes("HBASE_PUT_COL_A"));
        // 多RowKey=HBASE_PUT_ROW_KEY_A的这一行增加一列COL_NAME_B=HBASE_PUT_COL_B
        put.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_B),
                Bytes.toBytes("HBASE_PUT_COL_B"));
        try {
            // 数据的插入
            table.put(put);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    /**
     * 增加多行数据
     */
    public static void doPutMulRows() {
        HTableInterface table = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);

        // 构造多行实例
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
     * 进行多行插入的时候，其中一行出现数据异常，不会导致整个批量插入都失败
     * 
     * 注：
     * 这里的异常必须保证Put是完整的，即列族，列名，列值这三个元素不能缺少，否则还是会影响其他的插入
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
         * 构造一个Put，使用一个不存在的列族，这个Put的数据插入注定会失败，异常抛出 
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
     * 多行插入，但是使用了一个不完整的Put，这个不完整Put的插入失败会导致其他Put的插入也失败
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
         * 构造一个Put，但是没有指定列族，列以及列的值，这样会影响其他行数据的插入
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
     * 检查符合条件后再更新
     */
    public static void doCheckAndPut() {
        HTableInterface hTable = HBaseCommon.getTable(HBaseSharedContants.TEST_TABLE_NAME);
        Put putB = new Put(Bytes.toBytes(ROW_KEY_B));
        putB.add(Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME), Bytes.toBytes(COL_NAME_A),
                 Bytes.toBytes("HBASE_CHECK_AND_PUT_A"));

        try {
            /** 当列COL_NAME_A的值为NULL的时候更新列A的值，此时列COL_NAME_A的值肯定不是NULL,因此返回值一定是false **/
            boolean res = hTable.checkAndPut(Bytes.toBytes(ROW_KEY_B),
                                             Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME),
                                             Bytes.toBytes(COL_NAME_A), null, putB);
            System.out.println(res);

            /** 当列COL_NAME_C的值为NULL的时候更新列A的值，此时列COL_NAME_C的值为NULL,因此返回值一定是true **/
            res = hTable.checkAndPut(Bytes.toBytes(ROW_KEY_B), Bytes.toBytes(HBaseSharedContants.COLUMN_FAMILY_NAME),
                                     Bytes.toBytes(COL_NAME_C), null, putB);
            System.out.println(res);
        } catch (Exception e) {
            logger.error(e);
        }
    }

    public static void main(String[] args) {
        // 0. 给一行增加多个列，可以直接在HBASE的shell客户端使用 scan 'compnay_compare' 来查看新增的数据
        doPutSingleRow();
        // 1. 增加多行
        doPutMulRows();
        // 2. 增加多行，其中一行发生异常，不会导致其他行的增加也失败
        doPutMulRowsEx();
        // 3. 增加多行，但是其中一行是一个不完整的Put，此时会因为这个不完整的Put导致其他行的插入也失败
        doPutMulRowsExEx();
        // 4. 检查满足某一条件后，再执行更新
        doCheckAndPut();
    }
}
