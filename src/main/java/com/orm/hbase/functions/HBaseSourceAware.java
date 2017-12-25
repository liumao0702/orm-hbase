package com.orm.hbase.functions;

import com.orm.hbase.client.HBaseSource;

/**
 * @author zap
 * @version 1.0, 2017/12/22
 */
public interface HBaseSourceAware {

    void setHBaseSource(HBaseSource source);

}
