package com.orm.hbase.criteria;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public interface Operation<T> {

    Class<T> getPoClass();

}
