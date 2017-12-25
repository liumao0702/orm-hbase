package com.orm.hbase.type;

import com.google.common.base.Preconditions;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class ByteResolver extends BaseTypeResolver<Byte> {
    @Override
    public byte[] toBytes(Byte obj) {
        Preconditions.checkArgument(accept(obj), "转换类型不上Byte类型");
        return new byte[]{obj};
    }

    @Override
    public Byte toObj(byte[] bytes) {
        Preconditions.checkState(null != bytes && bytes.length > 1, "字节数组长度必须大于1");
        return bytes[0];
    }
}
