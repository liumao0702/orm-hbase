package com.orm.hbase.type;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class DateResolver extends BaseTypeResolver<Date> {
    @Override
    public byte[] toBytes(Date obj) {
        Preconditions.checkArgument(accept(obj), "转换类型不是Date类型");
        return Bytes.toBytes(obj.getTime());
    }

    @Override
    public Date toObj(byte[] bytes) {
        Preconditions.checkState(null != bytes && bytes.length > 1, "字节数组长度必须大于1");
        long time = Bytes.toLong(bytes);
        return new Date(time);
    }
}
