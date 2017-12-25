package com.orm.hbase.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.orm.hbase.exceptions.ORMHBaseException;
import com.orm.hbase.functions.Persistent;
import com.orm.hbase.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.io.IOException;
import java.util.List;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class HBaseClient implements Persistent {

    private HBaseSource source;

    public HBaseClient(HBaseSource source) {
        this.source = source;
    }

    @Override
    public <T> void put(T po) throws ORMHBaseException {
        Preconditions.checkNotNull(po, "保存对象不能为空");

        try (Table table = this.source.getTable(HBaseUtils.findTableName(po.getClass()))) {
            Put put = HBaseUtils.wrapPut(po);
            table.put(put);
        } catch (IOException e) {
            throw new ORMHBaseException("在保存数据的方法生异常", e);
        }
    }

    @Override
    public <T> void putList(List<T> poList) throws ORMHBaseException {
        Preconditions.checkState(null != poList && poList.size() > 0, "保存对象不能为空");

        try (Table table = this.source.getTable(HBaseUtils.findTableName(poList.get(0).getClass()))) {
            List<Put> puts = HBaseUtils.wrapPutList(poList);
            table.put(puts);
        } catch (IOException e) {
            throw new ORMHBaseException("在批量保存的方法生发生异常", e);
        }
    }

    @Override
    public <T> T find(byte[] rowkey, Class<T> classType) throws ORMHBaseException {
        Preconditions.checkNotNull(classType, "查询实例类型不能为空");

        try (Table table = this.source.getTable(HBaseUtils.findTableName(classType))) {

            Result result = table.get(HBaseUtils.wrapGet(rowkey));
            return HBaseUtils.wrapResult(classType, result);

        } catch (IOException e) {
            throw new ORMHBaseException("在获取单条数据的方法上发生异常", e);
        }
    }

    @Override
    public <T> List<T> findList(List<byte[]> rowkeyList, Class<T> classType) throws ORMHBaseException {

        Preconditions.checkNotNull(classType, "查询实例类型不能为空");

        try (Table table = this.source.getTable(HBaseUtils.findTableName(classType))) {

            List<Get> gets = Lists.newArrayList();
            for (byte[] rowKey : rowkeyList) {
                gets.add(HBaseUtils.wrapGet(rowKey));
            }

            return HBaseUtils.wrapResult(classType, table.get(gets));

        } catch (IOException e) {
            throw new ORMHBaseException("在批量获取数据的方法上发生异常", e);
        }
    }

    @Override
    public <T> List<T> findList(byte[] startRowKey, byte[] endRowKey, Class<T> classType) throws ORMHBaseException {
        Preconditions.checkNotNull(classType, "查询实例类型不能为空");

        try (Table table = this.source.getTable(HBaseUtils.findTableName(classType))) {

            Scan scan = constructScan(startRowKey, endRowKey);

            List<T> resultList = Lists.newArrayList();
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                T t = HBaseUtils.wrapResult(classType, result);
                resultList.add(t);
            }

            return resultList;

        } catch (IOException e) {
            throw new ORMHBaseException("在批量获取数据的方法上发生异常", e);
        }
    }

    @Override
    public void delete(byte[] rowkey, Class<?> classTpye) throws ORMHBaseException {
        Preconditions.checkNotNull(classTpye, "删除实例类型不能为空");

        try (Table table = this.source.getTable(HBaseUtils.findTableName(classTpye))) {

            table.delete(HBaseUtils.wrapDelete(rowkey));

        } catch (Exception e) {
            throw new ORMHBaseException("在删除的方法上发生异常", e);
        }
    }

    @Override
    public void deleteList(List<byte[]> rowKeyList, Class<?> classType) throws ORMHBaseException {
        Preconditions.checkNotNull(classType, "删除实例类型不能为空");

        try (Table table = this.source.getTable(HBaseUtils.findTableName(classType))) {

            List<Delete> deletes = Lists.newArrayList();
            for (byte[] rowKey : rowKeyList) {
                deletes.add(HBaseUtils.wrapDelete(rowKey));
            }

            table.delete(deletes);

        } catch (IOException e) {
            throw new ORMHBaseException("在批量删除的方法上发生异常", e);
        }

    }

    private Scan constructScan(byte[] startRowKey, byte[] endRowKey, Filter... filters) {
        Scan scan = new Scan();

        if (null != startRowKey) {
            scan.setStartRow(startRowKey);
        }

        if (null != endRowKey) {
            scan.setStopRow(endRowKey);
        }

        if (null != filters && filters.length > 0) {
            FilterList filterList = new FilterList(filters);
            scan.setFilter(filterList);
        }

        return scan;
    }

}
