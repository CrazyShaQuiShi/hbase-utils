package com.github.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @author: CrazyShaQiuShi
 * @description: Hbase连接工具类
 * @date: 2019/8/18 18:33
 * @version:1.0.0
 */
public class HbaseConn {
    private static final HbaseConn INSTANCE = new HbaseConn();
    private static Configuration configuration;
    private static Connection connection;

    private HbaseConn() {
        try {
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum","hbase001");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 获取hbase连接
     *
     * @return
     */
    public static Connection getHbaseConn() {
        return INSTANCE.getConnection();
    }

    /**
     * 获取表
     *
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    /**
     * 关闭连接
     */
    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
