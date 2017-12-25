package com.orm.hbase.criteria;

import java.util.List;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class PutOp<T> implements Operation<T> {

    private Class<T> poClass;
    private T po;
    private List<T> poList;

    @Override
    public Class<T> getPoClass() {
        return this.poClass;
    }

    public void setPoClass(Class<T> poClass) {
        this.poClass = poClass;
    }

    public T getPo() {
        return po;
    }

    public void setPo(T po) {
        this.po = po;
    }

    public List<T> getPoList() {
        return poList;
    }

    public void setPoList(List<T> poList) {
        this.poList = poList;
    }
}
