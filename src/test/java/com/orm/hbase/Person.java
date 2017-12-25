package com.orm.hbase;

import com.orm.hbase.annotations.Column;
import com.orm.hbase.annotations.RowKey;
import com.orm.hbase.annotations.Table;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
@Table(name = "test_table")
public class Person {

    @RowKey
    private String rowkey;

    @Column(family = "tf", qualifier = "dv")
    private String data;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
