package com.orm.hbase.config;

import java.lang.reflect.Field;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class ColumnSchema extends Schema {

    private String family;
    private String qualifier;
    private Field field;

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getQualifier() {
        return qualifier;
    }

    public void setQualifier(String qualifier) {
        this.qualifier = qualifier;
    }

    public Field getField() {
        return field;
    }

    public void setField(Field field) {
        this.field = field;
    }
}
