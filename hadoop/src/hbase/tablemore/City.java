package hbase.tablemore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.org.apache.xml.internal.security.Init;

public class City {
	static Configuration configuration = HBaseConfiguration.create();  
		 

	public static void main(String[] args) throws IOException {
		
		
		City.createTable("city");
		
		City.insertData("city","1","name","name","China");
		City.insertData("city", "1", "child", "2", "state");
		City.insertData("city", "1", "child", "3", "state");
		City.insertData("city", "1", "child", "4", "state");
		City.insertData("city", "1", "child", "5", "state");
		City.insertData("city", "1", "child", "6", "state");
		City.insertData("city","2","name","name","Beijing");
		City.insertData("city", "2", "parent", "1", "nation");
		City.insertData("city","3","name","name","Shanghai");
		City.insertData("city", "3", "parent", "1", "nation");
		City.insertData("city","4","name","name","Guangzhou");
		City.insertData("city", "4", "parent", "1", "nation");
		City.insertData("city","5","name","name","Shandong");
		City.insertData("city", "5", "parent", "1", "nation");
		City.insertData("city", "5", "child", "7", "city");
		City.insertData("city", "5", "child", "8", "city");
		City.insertData("city","6","name","name","Guangzhou");
		City.insertData("city", "6", "parent", "1", "nation");
		City.insertData("city", "6", "child", "9", "city");
		City.insertData("city","7","name","name","Jinan");
		City.insertData("city", "7", "parent", "1", "nation");
		City.insertData("city", "7", "parent", "5", "state");
		City.insertData("city","8","name","name","Qingdao");
		City.insertData("city", "8", "parent", "1", "nation");
		City.insertData("city", "8", "parent", "5", "state");
		City.insertData("city","9","name","name","Ghengdu");
		City.insertData("city", "9", "parent", "1", "nation");
		City.insertData("city", "9", "parent", "6", "state");
		
		

	}
	
	@SuppressWarnings({ "deprecation", "resource" })
	public static void createTable(String tableName) {  
        System.out.println("start create table ......");  
        try {  
        	HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  
                hBaseAdmin.disableTable(tableName);  
                hBaseAdmin.deleteTable(tableName);  
                System.out.println(tableName + " is exist,detele....");  
            }  
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
            tableDescriptor.addFamily(new HColumnDescriptor("name"));  
            tableDescriptor.addFamily(new HColumnDescriptor("parent"));  
            tableDescriptor.addFamily(new HColumnDescriptor("child"));  
            hBaseAdmin.createTable(tableDescriptor);  
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        System.out.println("end create table ......");  
    }  
	@SuppressWarnings({ "deprecation", "resource" })
	public static void insertData(String tablename, String row, String columnFamily, String column, String data)
			throws IOException {
		HTable table = new HTable(configuration, tablename);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
	}


	}
