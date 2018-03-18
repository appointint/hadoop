package hbase.tablemore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Friends {
	static Configuration configuration = HBaseConfiguration.create();  
	public static void main(String[] args) throws IOException {
		
		createTable("friends", "info", "user");
		for (int i = 0; i < 10; i++) {
			insertData("friends", ""+i, "info", "name", "");
			insertData("friends", ""+i, "info", "sex", "");
			insertData("friends", ""+i, "info", "age", "");
			for (int j = 0; j < 3; j++) {
				int num=(int)(Math.random()*10);	
				insertData("friends", ""+i, "user", "uid", ""+num);
			}
		}
		
		
		
	}
	
	@SuppressWarnings({ "deprecation", "resource" })
	public static void createTable(String tableName,String family1,String family2) {  
        System.out.println("start create table ......");  
        try {  
        	HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration); 
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  
                hBaseAdmin.disableTable(tableName);  
                hBaseAdmin.deleteTable(tableName);  
                System.out.println(tableName + " is exist,detele....");  
            }  
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
            tableDescriptor.addFamily(new HColumnDescriptor(family1));  
            tableDescriptor.addFamily(new HColumnDescriptor(family2));  
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
