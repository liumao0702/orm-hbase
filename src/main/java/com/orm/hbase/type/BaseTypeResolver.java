package com.orm.hbase.type;

import com.google.common.base.Preconditions;

import java.lang.reflect.ParameterizedType;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public abstract class BaseTypeResolver<T> implements TypeResolver<T> {

    @Override
    public boolean accept(Object obj) {
        Preconditions.checkNotNull(obj, "匹配对象不能为空");

        Class<?> type = (Class<?>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];

        return obj.getClass() == type;
    }
}
