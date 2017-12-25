package com.orm.hbase.functions;

import com.orm.hbase.exceptions.ORMHBaseException;

import java.util.List;

/**
 * @author zap
 * @version 1.0, 2017/12/22
 */
public interface Persistent {

    <T> void put(T po) throws ORMHBaseException;

    <T> void putList(List<T> poList) throws ORMHBaseException;

    <T> T find(byte[] rowkey, Class<T> classType) throws ORMHBaseException;

    <T> List<T> findList(List<byte[]> rowkeyList, Class<T> classType) throws ORMHBaseException;

    <T> List<T> findList(byte[] startRowKey, byte[] endRowKey, Class<T> classType) throws ORMHBaseException;

    void delete(byte[] rowkey, Class<?> classTpye) throws ORMHBaseException;

    void deleteList(List<byte[]> deleteList, Class<?> classType) throws ORMHBaseException;

}
