package com.orm.hbase;

import com.orm.hbase.client.HBaseClient;
import com.orm.hbase.client.HBaseSource;
import com.orm.hbase.criteria.Criteria;
import com.orm.hbase.exceptions.ORMHBaseException;
import com.orm.hbase.functions.Persistent;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class CriteriaTest {

    public static void main(String[] args) throws ORMHBaseException {
        Person person = new Person();

        Persistent client = createClient();
        Criteria.put(Person.class).put(person).build().exec(client);
    }

    private static HBaseClient createClient() throws ORMHBaseException {
        HBaseSource source = new HBaseSource();
        HBaseClient client = new HBaseClient(source);
        return client;
    }

}
