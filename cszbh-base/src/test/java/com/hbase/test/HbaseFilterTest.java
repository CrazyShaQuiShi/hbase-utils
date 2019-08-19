package com.hbase.test;

import com.github.hbase.HbaseUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author: CrazyShaQiuShi
 * @description: 测试类
 * @date: 2019/8/18 21:36
 * @version:1.0.0
 */
public class HbaseFilterTest {

    @Test
    public void rowFilterTest() {
        Filter filter = new RowFilter(CompareOperator.EQUAL, new BinaryComparator(Bytes.toBytes("rowKey0001")));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HbaseUtil.getScanner("FileTable", "rowKey0001", "rowKey0001", filterList);
        if (scanner != null) {
            scanner.forEach(sc->{
                System.out.println("rowKe=" + Bytes.toString(sc.getRow()));
                System.out.println("fileName=" + Bytes.toString(sc.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
            });
            //务必关闭防止内存泄漏
            scanner.close();
        }
    }
    @Test
    public void prefixFilterTest() {
        Filter filter = new PrefixFilter(Bytes.toBytes("rowKey"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HbaseUtil.getScanner("FileTable", "rowKey0001", "rowKey0003", filterList);
        if (scanner != null) {
            scanner.forEach(sc->{
                System.out.println("rowKe=" + Bytes.toString(sc.getRow()));
                System.out.println("fileName=" + Bytes.toString(sc.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
            });
            //务必关闭防止内存泄漏
            scanner.close();
        }
    }

    /**
     * 只返回key和列的值
     */
    @Test
    public void keyOnlyFilterTest() {
        Filter filter = new KeyOnlyFilter(true);
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HbaseUtil.getScanner("FileTable", "rowKey0001", "rowKey0003", filterList);
        if (scanner != null) {
            scanner.forEach(sc->{
                System.out.println("rowKe=" + Bytes.toString(sc.getRow()));
                System.out.println("fileName=" + Bytes.toString(sc.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
            });
            //务必关闭防止内存泄漏
            scanner.close();
        }
    }

    /**
     * 只返回key和列名
     */
    @Test
    public void columPrefixFilterTest() {
        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, Arrays.asList(filter));
        ResultScanner scanner = HbaseUtil.getScanner("FileTable", "rowKey0001", "rowKey0003", filterList);
        if (scanner != null) {
            scanner.forEach(sc->{
                System.out.println("rowKe=" + Bytes.toString(sc.getRow()));
                System.out.println("fileName=" + Bytes.toString(sc.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
                System.out.println("fileName=" + Bytes.toString(sc.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("type"))));
            });
            //务必关闭防止内存泄漏
            scanner.close();
        }
    }
}
