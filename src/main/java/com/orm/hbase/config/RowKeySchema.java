package com.orm.hbase.config;

import java.lang.reflect.Field;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class RowKeySchema extends Schema {

    private Field rowKeyField;

    public Field getRowKeyField() {
        return rowKeyField;
    }

    public void setRowKeyField(Field rowKeyField) {
        this.rowKeyField = rowKeyField;
    }
}
