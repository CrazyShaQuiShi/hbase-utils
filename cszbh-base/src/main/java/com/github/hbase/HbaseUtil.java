package com.github.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author: CrazyShaQiuShi
 * @description: Hbase工具类
 * @date: 2019/8/18 18:48
 * @version:1.0.0
 */
public class HbaseUtil {


    /**
     * 创建hbase表
     *
     * @param tableName 表名称
     * @param cfs       列族的数组
     * @return 是否创建成功
     */
    public static boolean createTable(String tableName, String[] cfs) {
        try {
            HBaseAdmin admin = (HBaseAdmin) HbaseConn.getHbaseConn().getAdmin();
            if (admin.tableExists(TableName.valueOf(tableName))) {
                return false;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            Arrays.stream(cfs).forEach(cf -> {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf);
                columnDescriptor.setMaxVersions(1);
                tableDescriptor.addFamily(columnDescriptor);
            });
            admin.createTable(tableDescriptor);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除表
     *
     * @param tableName 表名称
     * @return 是否操作成功
     */
    public static boolean deleteTable(String tableName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) HbaseConn.getHbaseConn().getAdmin();
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * @param tableName 表名称
     * @param rowKey    唯一标识
     * @param cfName    列族名
     * @param qualifier 列标识
     * @param data      数据
     * @return 是否操作成功
     */
    public static boolean putRow(String tableName, String rowKey, String cfName, String qualifier, String data) {
        try {
            Table table = HbaseConn.getTable(tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 批量添加
     *
     * @param tableName 表名
     * @param puts      put数组
     * @return 是否操作成功
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try {
            Table table = HbaseConn.getTable(tableName);
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 获取单条数据
     *
     * @param tableName 表名
     * @param rowKey    唯一标识
     * @return 查询结果
     */
    public static Result getRow(String tableName, String rowKey) {
        try {
            Table table = HbaseConn.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 根据过滤器查找具体数据
     *
     * @param tableName
     * @param rowKey
     * @param filterList
     * @return
     */
    public static Result getRow(String tableName, String rowKey, FilterList filterList) {
        try {
            Table table = HbaseConn.getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 全表扫描
     *
     * @param tableName
     * @return
     */
    public static ResultScanner getScanner(String tableName) {

        try {
            Table table = HbaseConn.getTable(tableName);
            Scan scan = new Scan();
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 批量检索
     *
     * @param tableName   表名
     * @param startRowKey 起始rowKey
     * @param endRowKey   终止rowKey
     * @return ResultScanner实例
     */
    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey) {

        try {
            Table table = HbaseConn.getTable(tableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(startRowKey));
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 批量检索
     *
     * @param tableName   表名
     * @param startRowKey 起始rowKey
     * @param endRowKey   终止rowKey
     * @param filterList  过滤器
     * @return ResultScanner 实例
     */
    public static ResultScanner getScanner(String tableName, String startRowKey, String endRowKey, FilterList filterList) {

        try {
            Table table = HbaseConn.getTable(tableName);
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRowKey));
            scan.setStopRow(Bytes.toBytes(startRowKey));
            scan.setFilter(filterList);
            scan.setCaching(1000);
            return table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除操作
     *
     * @param tableName 表名
     * @param rowKey    rowKey
     * @return 是否操作成功
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try {
            Table table = HbaseConn.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除操作 列族
     *
     * @param tableName 表名
     * @param cfName    rowKey
     * @return 是否操作成功
     */
    public static boolean deleteColumnFamily(String tableName, String cfName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) HbaseConn.getHbaseConn().getAdmin();
            admin.deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(cfName));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列
     *
     * @param tableName  表名
     * @param rowKey     rowKey
     * @param cfName     列族名称
     * @param qualifiter 删除操作 列
     * @return
     */
    public static boolean deleteQualifiter(String tableName, String rowKey, String cfName, String qualifiter) {
        try {
            Table table = HbaseConn.getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifiter));
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

}
