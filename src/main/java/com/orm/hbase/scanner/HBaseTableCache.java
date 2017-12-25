package com.orm.hbase.scanner;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.orm.hbase.config.Schema;
import com.orm.hbase.exceptions.ORMHBaseException;
import com.orm.hbase.utils.HBaseUtils;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author zap
 * @version 1.0, 2017/12/22
 */
public class HBaseTableCache {

    private LoadingCache<Class<?>, List<Schema>> tablePool;

    private volatile static HBaseTableCache tableCache = null;

    private HBaseTableCache() {
        initialize();
    }

    public static HBaseTableCache instance() {
        if (null == tableCache) {
            synchronized (HBaseTableCache.class) {
                if (null == tableCache) {
                    tableCache = new HBaseTableCache();
                }
            }
        }

        return tableCache;
    }

    public List<Schema> get(Class<?> key) throws ORMHBaseException {
        try {
            return tablePool.get(key);
        } catch (ExecutionException e) {
            throw new ORMHBaseException("获取表映射发生异常");
        }
    }

    public void put(Class<?> key, List<Schema> value) {
        tablePool.put(key, value);
    }

    public long size() {
        return tablePool.size();
    }

    private void initialize() {
        tablePool = CacheBuilder.newBuilder()
                .maximumSize(100)
                .build(new CacheLoader<Class<?>, List<Schema>>() {
                    @Override
                    public List<Schema> load(Class<?> key) throws Exception {
                        return HBaseUtils.findColumnSchemaList(key);
                    }
                });
    }


}
