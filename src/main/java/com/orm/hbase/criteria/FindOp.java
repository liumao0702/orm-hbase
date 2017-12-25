package com.orm.hbase.criteria;

import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class FindOp<T> implements Operation<T> {

    private Class<T> poClass;
    private byte[] rowKey;
    private List<byte[]> rowKeyList;
    private byte[] startRow;
    private byte[] endRow;

    private Filter[] filters;

    @Override
    public Class<T> getPoClass() {
        return this.poClass;
    }

    public void setPoClass(Class<T> poClass) {
        this.poClass = poClass;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public List<byte[]> getRowKeyList() {
        return rowKeyList;
    }

    public void setRowKeyList(List<byte[]> rowKeyList) {
        this.rowKeyList = rowKeyList;
    }

    public byte[] getStartRow() {
        return startRow;
    }

    public void setStartRow(byte[] startRow) {
        this.startRow = startRow;
    }

    public byte[] getEndRow() {
        return endRow;
    }

    public void setEndRow(byte[] endRow) {
        this.endRow = endRow;
    }

    public Filter[] getFilters() {
        return filters;
    }

    public void setFilters(Filter[] filters) {
        this.filters = filters;
    }
}
