package com.orm.hbase.type;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Date;
import java.util.Map;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class TypeResolverFactory {

    private static Map<Class<?>, TypeResolver<?>> defaultResolverMap = Maps.newHashMap();

    static {
        defaultResolverMap.put(String.class, new StringResolver());

        defaultResolverMap.put(Date.class, new DateResolver());

        BooleanResolver booleanResolver = new BooleanResolver();
        defaultResolverMap.put(boolean.class, booleanResolver);
        defaultResolverMap.put(Boolean.class, booleanResolver);

        ByteResolver byteResolver = new ByteResolver();
        defaultResolverMap.put(byte.class, byteResolver);
        defaultResolverMap.put(Byte.class, byteResolver);

        ShortResolver shortResolver = new ShortResolver();
        defaultResolverMap.put(short.class, shortResolver);
        defaultResolverMap.put(Short.class, shortResolver);

        IntegerResolver integerResolver = new IntegerResolver();
        defaultResolverMap.put(int.class, integerResolver);
        defaultResolverMap.put(Integer.class, integerResolver);

        LongResolver longResolver = new LongResolver();
        defaultResolverMap.put(long.class, longResolver);
        defaultResolverMap.put(Long.class, longResolver);

        FloatResolver floatResolver = new FloatResolver();
        defaultResolverMap.put(float.class, floatResolver);
        defaultResolverMap.put(Float.class, floatResolver);

    }

    public static TypeResolver<?> getResolver(Class<?> clazz) {
        Preconditions.checkNotNull(clazz, "类型不能为空");

        return defaultResolverMap.get(clazz);
    }

}
