package com.orm.hbase.client;

import com.orm.hbase.exceptions.ORMHBaseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Properties;

/**
 * @author zap
 * @version 1.0, 2017/12/22
 */
public class HBaseSource {

    private Configuration conf;
    private Connection conn;
    private Properties config;

    public HBaseSource() throws ORMHBaseException {
        init();
    }

    public HBaseSource(Properties properties) throws ORMHBaseException {
        this.config = properties;
        init();
    }

    private void init() throws ORMHBaseException {
        conf = HBaseConfiguration.create();

        try {
            if (null != config) {
                config.keySet().forEach(key -> {
                    conf.set(key.toString(), config.getProperty(key.toString()));
                });
            }
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new ORMHBaseException("创建HBase连接失败", e);
        }
    }

    public Configuration getConf() {
        return this.conf;
    }

    public Connection getConn() throws IOException {
        if (null == this.conn || this.conn.isClosed()) {
            this.conn = ConnectionFactory.createConnection(conf);
        }
        return this.conn;
    }

    public Table getTable(String tableName) throws ORMHBaseException {
        try {
            return getConn().getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new ORMHBaseException("HBase表获取失败", e);
        }
    }

    public void closeTable(Table table) throws ORMHBaseException {
        if (null != table) {
            try {
                table.close();
            } catch (IOException e) {
                throw new ORMHBaseException("关闭HBase表失败", e);
            }
        }
    }

}
