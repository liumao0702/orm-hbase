package com.orm.hbase.type;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class ShortResolver extends BaseTypeResolver<Short> {
    @Override
    public byte[] toBytes(Short obj) {
        Preconditions.checkArgument(accept(obj), "转换类型不是Short类型");
        return Bytes.toBytes(obj);
    }

    @Override
    public Short toObj(byte[] bytes) {
        Preconditions.checkState(null != bytes && bytes.length > 1, "字节数组长度必须大于1");
        return Bytes.toShort(bytes);
    }
}
