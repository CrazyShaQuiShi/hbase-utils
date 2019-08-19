package com.hbase.test;

import com.github.hbase.HbaseConn;
import com.github.hbase.HbaseUtil;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class HbaseTest {

    /**
     * 测试数据库连接
     */
    @Test
    public void getConnectionTest() {
        Connection connection = HbaseConn.getHbaseConn();
        System.out.println(connection.isClosed());
        HbaseConn.closeConn();
        System.out.println(connection.isClosed());
    }

    @Test
    public void tableTest() {
        try {
            Table table = HbaseConn.getTable("FileTable");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void getRowTest() {
        Result result = HbaseUtil.getRow("test", "row1");
        System.out.println(result.getRow());
    }

    /**
     * 测试创建表
     */
    @Test
    public void createTableTest() {
        HbaseUtil.createTable("FileTable", new String[]{"fileInfo", "saveInfo"});
    }

    /**
     * 添加文件详情
     */
    @Test
    public void addFileDetails() {
        HbaseUtil.putRow("FileTable", "rowKey0001", "fileInfo", "name", "file.txt");
        HbaseUtil.putRow("FileTable", "rowKey0001", "fileInfo", "type", "txt");
        HbaseUtil.putRow("FileTable", "rowKey0001", "fileInfo", "size", "1024");
        HbaseUtil.putRow("FileTable", "rowKey0001", "fileInfo", "creator", "szb");

        HbaseUtil.putRow("FileTable", "rowKey0003", "fileInfo", "name", "file3.txt");
        HbaseUtil.putRow("FileTable", "rowKey0003", "fileInfo", "type", "txt");
        HbaseUtil.putRow("FileTable", "rowKey0003", "fileInfo", "size", "1024");
        HbaseUtil.putRow("FileTable", "rowKey0003", "fileInfo", "creator", "szb");
    }

    @Test
    public void fileDetailsTest() {
        Result result = HbaseUtil.getRow("FileTable", "rowKey0001");
        if (result != null) {
            System.out.println("rowKe=" + Bytes.toString(result.getRow()));
            System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
        }
    }

    /**
     * scan测试
     */
    @Test
    public void fileDetailsFilter() {
        ResultScanner scanner = HbaseUtil.getScanner("FileTable", "rowKey0001","rowKey0001");
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
     * 删除行
     */
    @Test
    public void deleteRowTest(){
        HbaseUtil.deleteRow("FileTable","rowKey0001");
        Result result = HbaseUtil.getRow("FileTable", "rowKey0001");
        if (result != null) {
            System.out.println("rowKe=" + Bytes.toString(result.getRow()));
            System.out.println("fileName=" + Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("name"))));
        }
    }
}
