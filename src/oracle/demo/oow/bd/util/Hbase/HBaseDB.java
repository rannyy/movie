package oracle.demo.oow.bd.util.Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;



public class HBaseDB {

	private Connection conn;
	
	private static class HbaseDBInstance{
		private static final HBaseDB instance=new HBaseDB();
	}
	
	public static HBaseDB getInstance() {
		//return new HBaseDB();
		return HbaseDBInstance.instance;
	}
	public void closeCoon(){
		try {
			conn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//私有的防治重新建立
	private HBaseDB() {
		//获取配置类对象
		Configuration conf = HBaseConfiguration.create();
		//指定zookeeper地址
		conf.set("hbase.zookeeper.quorum", "lei");
		//指定hbase存储根目录
		conf.set("hbase.rootdir", "hdfs://lei:9000/hbase");
		try {
			//获取hbase数据库链接
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据表名称和列族创建表
	 * @param tableName
	 * @param columnFamilies
	 */
	public void createTable(String tableName, String[] columnFamilies) {
		deleteTable(tableName);
		try {
			Admin admin = conn.getAdmin();
			//指定表名称
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			//添加列族
			for (String string : columnFamilies) {
				HColumnDescriptor family = new HColumnDescriptor(Bytes.toBytes(string));
				descriptor.addFamily(family);
			}
			admin.createTable(descriptor);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据表名称删除表
	 * @param tableName
	 */
	public void deleteTable(String tableName) {
		try {
			Admin admin = conn.getAdmin();
			if(admin.tableExists(TableName.valueOf(tableName))) {
				//首先disabled
				admin.disableTable(TableName.valueOf(tableName));
				//drop
				admin.deleteTable(TableName.valueOf(tableName));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 根据表名称获取table对象
	 * @param tableName
	 * @return
	 */
	public Table getTable(String tableName) {
		try {
			return conn.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	/*
	 * 
	 * 根据计算器计算行建
	 * 
	 */
	public Long getId(String tableName, String family,String qualifier) {
	 
		Table table = getTable(tableName);
		 try {
			return table.incrementColumnValue(Bytes.toBytes(ConstantsHBase.ROW_KEY_GID_ACTIVITY_ID), Bytes.toBytes(family), Bytes.toBytes(qualifier), 1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return (long) 01;
	}
	
	public void put(String tablename ,Integer rowkey,String family,String qualifier,String value){
		Table table = getTable(tablename);
		Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),  Bytes.toBytes(value));
	    try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void put(String tablename, int rowkey, String family,	String qualifier, int value) {
		Table table = getTable(tablename);
		Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),  Bytes.toBytes(value));
	    try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void put(String tablename, int rowkey, String family,	String qualifier, double value) {
		Table table = getTable(tablename);
		Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),  Bytes.toBytes(value));
	    try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void put(String tablename ,String rowkey,String family,String qualifier,String value){
		Table table = getTable(tablename);
		Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),  Bytes.toBytes(value));
	    try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void put(String tablename ,String rowkey,String family,String qualifier,int value){
		Table table = getTable(tablename);
		Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),  Bytes.toBytes(value));
	    try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
