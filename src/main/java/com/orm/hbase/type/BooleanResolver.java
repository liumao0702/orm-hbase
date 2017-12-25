package com.orm.hbase.type;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class BooleanResolver extends BaseTypeResolver<Boolean> {
    @Override
    public byte[] toBytes(Boolean obj) {
        Preconditions.checkArgument(accept(obj), "转换类型不是Boolean类型");
        return Bytes.toBytes(obj);
    }

    @Override
    public Boolean toObj(byte[] bytes) {
        Preconditions.checkState(null != bytes && bytes.length > 0, "字节数组长度不能小于1");
        return Bytes.toBoolean(bytes);
    }
}
