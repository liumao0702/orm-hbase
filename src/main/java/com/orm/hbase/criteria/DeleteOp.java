package com.orm.hbase.criteria;

import java.util.List;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class DeleteOp<T> implements Operation<T> {

    private Class<T> poClass;
    private byte[] rowKey;
    private List<byte[]> rowKeyList;

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
}
