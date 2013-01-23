package com.infy.hbase.utilities;

/* © 2012 Infosys Technologies Limited, Bangalore, India. All rights reserved.
 * Version: 1.0
 * Except for any open source software components embedded in this Infosys
 * proprietary software program (“Program”), this Program is protected by
 * copyright laws, international treaties and other pending or existing
 * intellectual property rights in India, the United States and other countries.
 * Except as expressly permitted, any unauthorized reproduction, storage,
 * transmission in any form or by any means (including without limitation
 * electronic, mechanical, printing, photocopying, recording or otherwise),
 * or any distribution of this Program, or any portion of it, may result in
 * severe civil and criminal penalties, and will be prosecuted to the maximum
 * extent possible under the law.*/

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.infy.hbase.context.ApplicationContextHelper;

public class HBaseTemplate {
	private HTablePool hTablePool;
	private static HBaseTemplate instance = null;
	private static final Object creationLock = new Object();
	
	private HBaseTemplate() {
		// TODO Auto-generated constructor stub
	}
	
    public static HBaseTemplate getInstance() {
    	if(instance == null) {
			synchronized (creationLock) {
				if(instance == null) {
					instance = new HBaseTemplate(); 
					HTablePool hTablePool =	(HTablePool)ApplicationContextHelper.getBean("hTablePool");
					instance.setHTablePool(hTablePool);
				}
			}
    	}
    	return instance;
    }
	/*public HBaseTemplate(HTablePool hTablePool) {
        this.hTablePool = hTablePool;
    }*/
	public HTablePool getHTablePool() {
        return hTablePool;
    }
    public void setHTablePool(HTablePool tablePool) {
        hTablePool = tablePool;
    }
    
    /* override all required methods from HTable class*/
    public Result get(String tableName, Get get) throws IOException {
    	HTableInterface hTable = hTablePool.getTable(tableName);
    	Result result = hTable.get(get);
    	hTable.close();
    	return result;
    }
    
    public Result[] get(String tableName, List<Get> get) throws IOException {
    	HTableInterface hTable = hTablePool.getTable(tableName);
    	Result[] result = hTable.get(get);
    	hTable.close();
    	return result;
    }
    public void put(String tableName, Put put) throws IOException {
    	HTableInterface hTable = hTablePool.getTable(tableName);
    	hTable.put(put);
    	hTable.close();
    }
    public void put(String tableName, List<Put> put) throws IOException {
    	HTableInterface hTable = hTablePool.getTable(tableName);
    	hTable.put(put);
    	hTable.close();
    }
    
   
    public void delete(String tableName, Delete delete) throws IOException {
    	HTableInterface hTable = hTablePool.getTable(tableName);
    	hTable.delete(delete);
    	hTable.close();
	}
    public void delete(String tableName, List<Delete> delete) throws IOException {
    	HTableInterface hTable = hTablePool.getTable(tableName);
    	hTable.delete(delete);
    	hTable.close();
	}
    /* ResultScanner has to be closed by the calling application 
     * explicitly by using ResultScanner.close() method
     */
    public ResultScanner getScanner(String tableName, Scan scan) throws IOException {
    	HTableInterface hTable = hTablePool.getTable(tableName);
    	ResultScanner resultScanner = hTable.getScanner(scan);
    	hTable.close();
    	return resultScanner;
    }

}
