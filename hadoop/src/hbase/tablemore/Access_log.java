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

public class Access_log {
	static Configuration configuration = HBaseConfiguration.create();  
	public static void main(String[] args) throws IOException {
		
		createTable("accesslog","http","user");
		

		for (int i = 0; i < 10; i++) {
			long time=System.currentTimeMillis();
			insertData("accesslog", time+"|"+i, "http", "ip", "");
			insertData("accesslog", time+"|"+i, "http", "domain", "");
			insertData("accesslog", time+"|"+i, "http", "url", "");
			insertData("accesslog", time+"|"+i, "http", "referer", "");
			insertData("accesslog", time+"|"+i, "user", "brower_cookie", "");
			insertData("accesslog", time+"|"+i, "user", "login_id", "");
			
			insertData("accesslog", time+"|"+i+"0", "http", "ip", "");
			insertData("accesslog", time+"|"+i+"0", "http", "domain", "");
			insertData("accesslog", time+"|"+i+"0", "http", "url", "");
			insertData("accesslog", time+"|"+i+"0", "http", "referer", "");
			insertData("accesslog", time+"|"+i+"0", "user", "brower_cookie", "");
			insertData("accesslog", time+"|"+i+"0", "user", "login_id", "");
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
