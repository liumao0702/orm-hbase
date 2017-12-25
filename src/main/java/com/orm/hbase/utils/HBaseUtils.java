package com.orm.hbase.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.orm.hbase.annotations.Table;
import com.orm.hbase.config.ColumnSchema;
import com.orm.hbase.config.RowKeySchema;
import com.orm.hbase.config.Schema;
import com.orm.hbase.exceptions.ORMHBaseException;
import com.orm.hbase.scanner.HBaseTableCache;
import com.orm.hbase.type.TypeResolver;
import com.orm.hbase.type.TypeResolverFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.List;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class HBaseUtils {

    public static Delete wrapDelete(byte[] rowKey) {
        Preconditions.checkNotNull(rowKey, "行键不能为空");

        return new Delete(rowKey);
    }

    public static Get wrapGet(byte[] rowKey) {
        Preconditions.checkNotNull(rowKey, "行键不能为空");

        return new Get(rowKey);
    }

    public static <T> Put wrapPut(T po) throws ORMHBaseException {
        Preconditions.checkNotNull(po, "保存实例不能为空");

        List<T> puts = Lists.newArrayList();
        puts.add(po);
        return wrapPutList(puts).get(0);
    }

    public static <T> List<Put> wrapPutList(List<T> poList) throws ORMHBaseException {
        Preconditions.checkNotNull(poList, "保存实例不能为空");
        Preconditions.checkArgument(poList.size() > 0, "保存实例列表不能为空");

        // 解析pojo中映射的HBase表信息
        List<Schema> schemaList = HBaseTableCache.instance().get(poList.get(0).getClass());


        // 找到行键属性
        RowKeySchema rowKeySchema = extractRowKeySchema(schemaList);

        List<Put> puts = Lists.newArrayList();
        for (T po : poList) {

            try {
                byte[] rowKey = resolverToBytes(rowKeySchema.getFieldType(), rowKeySchema.getRowKeyField().get(po));

                Put put = new Put(rowKey);

                // 设置列族和列
                for (Schema schema : schemaList) {
                    if (!(schema instanceof ColumnSchema)) {
                        continue;
                    }

                    ColumnSchema columnSchema = (ColumnSchema) schema;
                    Object valObj = columnSchema.getField().get(po);
                    if (null != valObj) {
                        byte[] fieldVal = resolverToBytes(columnSchema.getFieldType(), valObj);

                        put.addColumn(Bytes.toBytes(columnSchema.getFamily()),
                                Bytes.toBytes(columnSchema.getQualifier()), fieldVal);
                    }
                }

                puts.add(put);

            } catch (Exception e) {
                throw new ORMHBaseException(e);
            }

        }

        return puts;
    }

    public static List<Schema> findColumnSchemaList(Class<?> clazz) {
        List<Schema> schemaList = Lists.newArrayList();

        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);

            Schema schema = ColumnSchema.parse(field);
            if (null != schema) {
                schemaList.add(schema);
            }
        }

        return schemaList;
    }

    public static String findTableName(Class<?> clazz) {
        Preconditions.checkNotNull(clazz, "类型不能为空");

        Table table = clazz.getAnnotation(Table.class);
        Preconditions.checkState(null == table, "类型上必须使用Table注解");

        String tableName = table.name();
        if (StringUtils.isBlank(tableName)) {
            return clazz.getSimpleName();
        }

        return tableName;
    }

    public static <T> T wrapResult(Class<T> type, Result result) throws ORMHBaseException {
        Result[] results = {result};
        List<T> resultList = wrapResult(type, results);
        return resultList.size() > 0 ? resultList.get(0) : null;
    }

    public static <T> List<T> wrapResult(Class<T> classType, Result[] results) throws ORMHBaseException {
        // 获取表的schema
        List<Schema> schemaList = HBaseTableCache.instance().get(classType);

        // 提取行键属性
        RowKeySchema rowKeySchema = extractRowKeySchema(schemaList);

        List<T> resultList = Lists.newArrayList();

        for (Result result : results) {
            try {
                // 实例化对象
                T target = classType.newInstance();

                Cell[] cells = result.rawCells();
                if (null == cells || cells.length == 0) {
                    continue;
                }

                // 使用上面得到的行键属性，来为对象上相应的属性设置值
                TypeResolver<?> rowResolver = TypeResolverFactory.getResolver(rowKeySchema.getFieldType());
                rowKeySchema.getRowKeyField().set(target, rowResolver.toObj(result.getRow()));

                // 设置列
                for (Schema schema : schemaList) {
                    if (!(schema instanceof ColumnSchema)) {
                        continue;
                    }

                    ColumnSchema columnSchema = (ColumnSchema) schema;
                    Cell cell = result.getColumnLatestCell(Bytes.toBytes(columnSchema.getFamily()),
                            Bytes.toBytes(columnSchema.getQualifier()));
                    if (null != cell) {
                        TypeResolver<?> fieldResolver = TypeResolverFactory.getResolver(schema.getFieldType());
                        columnSchema.getField().set(target, fieldResolver.toObj(CellUtil.cloneValue(cell)));
                    }
                }

            } catch (Exception e) {
                throw new ORMHBaseException(e);
            }
        }

        return resultList;

    }

    @SuppressWarnings("all")
    private static byte[] resolverToBytes(Class<?> fieldType, Object fieldVal) throws ORMHBaseException {
        Preconditions.checkNotNull(fieldVal, "属性未设置");

        TypeResolver resolver = TypeResolverFactory.getResolver(fieldType);
        if (null == resolver) {
            throw new ORMHBaseException("不支持的类型:" + fieldType.getName());
        }

        return resolver.toBytes(fieldVal);

    }

    private static RowKeySchema extractRowKeySchema(List<Schema> schemaList) throws ORMHBaseException {
        Preconditions.checkArgument(schemaList.size() > 0, "表格列表不能为空");

        for (Schema schema : schemaList) {
            if (schema instanceof RowKeySchema) {
                return (RowKeySchema) schema;
            }
        }

        throw new ORMHBaseException("未找到行键属性");
    }

}
