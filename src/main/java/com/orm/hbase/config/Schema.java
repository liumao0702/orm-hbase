package com.orm.hbase.config;

import com.google.common.base.Preconditions;
import com.orm.hbase.annotations.Column;
import com.orm.hbase.annotations.RowKey;
import org.apache.commons.lang.StringUtils;

import java.lang.reflect.Field;

/**
 * @author zap
 * @version 1.0, 2017/12/22
 */
public abstract class Schema {

    private Class<?> fieldType;

    public Class<?> getFieldType() {
        return fieldType;
    }

    public void setFieldType(Class<?> fieldType) {
        this.fieldType = fieldType;
    }

    public static Schema parse(Field field) {
        if (field.isAnnotationPresent(RowKey.class)) {

            // 如果是行键属性，只需要记录属性类型和属性
            RowKeySchema rowKeySchema = new RowKeySchema();
            rowKeySchema.setFieldType(field.getType());
            rowKeySchema.setRowKeyField(field);
            return rowKeySchema;

        } else if (field.isAnnotationPresent(Column.class)) {

            // 如果是列属性，需要记录列族和列名、属性类型、属性
            Column column = field.getAnnotation(Column.class);
            String family = column.family();
            Preconditions.checkState(StringUtils.isNotBlank(family), "HBase列族不能为空");

            ColumnSchema columnSchema = new ColumnSchema();
            columnSchema.setFieldType(field.getType());
            columnSchema.setField(field);

            columnSchema.setFamily(family);
            columnSchema.setQualifier(column.qualifier());

            return columnSchema;
        }

        return null;
    }

}
