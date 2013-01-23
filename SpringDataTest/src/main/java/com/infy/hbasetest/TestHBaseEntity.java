
package com.infy.hbasetest;

import com.infy.hbase.entity.AbstractHBaseEntity;
import com.infy.hbase.entity.annotations.HBaseTable;

@HBaseTable(name = "hbase_table")
public class TestHBaseEntity
    extends AbstractHBaseEntity
{

    private String partNumber;

}
