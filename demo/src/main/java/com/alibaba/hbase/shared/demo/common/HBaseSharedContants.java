/*
 * Copyright 2013 Alibaba.com All right reserved. This software is the
 * confidential and proprietary information of Alibaba.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Alibaba.com.
 */
package com.alibaba.hbase.shared.demo.common;

/**
 * 类HBaseSharedContants.java的实现描述：一些公共常量
 * 
 * @author yangbolin Nov 6, 2013 4:22:59 PM
 */
public class HBaseSharedContants {

    /** 进制外部构造此类型的实例 **/
    private HBaseSharedContants(){
    }

    /** 表名 **/
    public static final String TEST_TABLE_NAME    = "company_compare";
    /** 列族 **/
    public static final String COLUMN_FAMILY_NAME = "compare_cf";
    /** HBASE连接池的大小 **/
    public static final int    HBASE_POOL_SIZE    = 1000;
}
