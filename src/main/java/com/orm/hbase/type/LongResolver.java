package com.orm.hbase.type;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class LongResolver extends BaseTypeResolver<Long> {
    @Override
    public byte[] toBytes(Long obj) {
        Preconditions.checkArgument(accept(obj), "转换类型不是Long类型");
        return Bytes.toBytes(obj);
    }

    @Override
    public Long toObj(byte[] bytes) {
        Preconditions.checkState(null != bytes && bytes.length > 1, "字节数组长度必须大于1");
        return Bytes.toLong(bytes);
    }
}
