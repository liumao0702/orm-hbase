package com.orm.hbase.criteria;

import com.google.common.base.Preconditions;
import com.orm.hbase.exceptions.ORMHBaseException;
import com.orm.hbase.functions.Persistent;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.List;

/**
 * Criteria操作
 *
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class Criteria {

    private Operation<?> op;

    private Criteria(Operation<?> op) {
        this.op = op;
    }

    public static <T> CriteriaPutBuilder<T> put(Class<T> clazz) {
        Preconditions.checkNotNull(clazz, "保存类型不能为空");
        return new CriteriaPutBuilder<>(clazz);
    }

    public static <T> CriteriaFindBuilder<T> find(Class<T> clazz) {
        Preconditions.checkNotNull(clazz, "查询类型不能为空");
        return new CriteriaFindBuilder<>(clazz);
    }

    public static <T> CriteriaDeleteBuilder<T> delete(Class<T> clazz) {
        Preconditions.checkNotNull(clazz, "删除类型不能为空");
        return new CriteriaDeleteBuilder<>(clazz);
    }

    /**
     * 执行命令，不需要返回值，如Put和Delete
     */
    public void exec(Persistent client) throws ORMHBaseException {
        if (op instanceof PutOp) {
            PutOp<?> putOp = (PutOp<?>) op;

            if (null != putOp.getPoList()) {
                client.putList(putOp.getPoList());
            } else {
                client.put(putOp.getPo());
            }
        } else if (op instanceof DeleteOp) {
            DeleteOp<?> delOp = (DeleteOp<?>) op;
            if (null != delOp.getRowKeyList()) {
                client.deleteList(delOp.getRowKeyList(), delOp.getPoClass());
            } else {
                client.delete(delOp.getRowKey(), delOp.getPoClass());
            }
        }
        throw new ORMHBaseException("不支持的操作");
    }

    /**
     * 查询单条数据
     */
    public <T> T query(Persistent client) throws ORMHBaseException {
        if (op instanceof FindOp) {
            @SuppressWarnings("all")
            FindOp<T> findOp = (FindOp<T>) op;

            if (null != findOp.getRowKey()) {
                return client.find(findOp.getRowKey(), findOp.getPoClass());
            }
        }
        throw new ORMHBaseException("不支持的操作");
    }

    /**
     * 查询多条数据
     */
    public <T> List<T> queryList(Persistent client) throws ORMHBaseException {
        if (op instanceof FindOp) {
            @SuppressWarnings("unchecked")
            FindOp<T> findOp = (FindOp<T>) op;

            if (null != findOp.getRowKeyList()) {
                return client.findList(findOp.getRowKeyList(), findOp.getPoClass());
            } else {
                client.findList(findOp.getStartRow(), findOp.getEndRow(), findOp.getPoClass());
            }
        }

        throw new ORMHBaseException("不支持的操作");
    }

    /**
     * 添加数据
     *
     * @param <T>
     */
    public static class CriteriaPutBuilder<T> {
        private PutOp<T> putOp;

        CriteriaPutBuilder(Class<T> putClass) {
            this.putOp = new PutOp<>();
            putOp.setPoClass(putClass);
        }

        public CriteriaPutBuilder<T> put(T obj) {
            putOp.setPo(obj);
            return this;
        }

        public CriteriaPutBuilder<T> putList(List<T> listObj) {
            putOp.setPoList(listObj);
            return this;
        }

        public Criteria build() {
            return new Criteria(putOp);
        }

    }

    /**
     * 查找数据
     *
     * @param <T>
     */
    public static class CriteriaFindBuilder<T> {
        private FindOp<T> findOp;

        CriteriaFindBuilder(Class<T> clazz) {
            findOp = new FindOp<>();
            findOp.setPoClass(clazz);
        }

        public CriteriaFindBuilder<T> rowKey(byte[] rowKey) {
            findOp.setRowKey(rowKey);
            return this;
        }

        public CriteriaFindBuilder<T> rowKeyList(List<byte[]> rowKeyList) {
            findOp.setRowKeyList(rowKeyList);
            return this;
        }

        public CriteriaFindBuilder<T> startRow(byte[] rowKey) {
            findOp.setStartRow(rowKey);
            return this;
        }

        public CriteriaFindBuilder<T> endRow(byte[] rowKey) {
            findOp.setEndRow(rowKey);
            return this;
        }

        public CriteriaFindBuilder<T> filters(Filter[] filters) {
            findOp.setFilters(filters);
            return this;
        }

        public Criteria build() {
            return new Criteria(findOp);
        }

    }

    /**
     * 删除数据
     *
     * @param <T>
     */
    public static class CriteriaDeleteBuilder<T> {
        private DeleteOp<T> deleteOp;

        CriteriaDeleteBuilder(Class<T> clazz) {
            deleteOp = new DeleteOp<>();
            deleteOp.setPoClass(clazz);
        }

        public CriteriaDeleteBuilder<T> rowKey(byte[] rowKey) {
            deleteOp.setRowKey(rowKey);
            return this;
        }

        public CriteriaDeleteBuilder<T> rowKeyList(List<byte[]> rowKeyList) {
            deleteOp.setRowKeyList(rowKeyList);
            return this;
        }

        public Criteria build() {
            return new Criteria(deleteOp);
        }

    }

}
