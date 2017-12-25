package com.orm.hbase.type;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public interface TypeResolver<T> {

    /**
     * 对象到字节数组
     *
     * @param obj 对象
     * @return 字节数组
     */
    byte[] toBytes(T obj);

    /**
     * 字节数组到对象
     *
     * @param bytes 字节数组
     * @return 对象
     */
    T toObj(byte[] bytes);

    /**
     * 检测对象的类型是否和泛型类型一致
     *
     * @param obj 对象实例
     * @return 匹配类型是否一致，一直返回true，否则返回false
     */
    boolean accept(Object obj);

}
