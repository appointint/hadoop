package hbase.baseconvert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public class Regionsplit {
	
	private static final Logger log = LoggerFactory.getLogger(RegionPair.class);
	 private static Configuration conf=HBaseConfiguration.create();
	 
	 public static void main(String[] args) throws IOException {
		 List<String> list=new ArrayList<String>();
		 list.add("tags");
		 
		 HBaseAdmin Admin = new HBaseAdmin(conf);
	       String tablename="region_user_test";
			if (Admin.tableExists(tablename)) {
				try {
					Admin.disableTable(tablename);
					Admin.deleteTable(tablename);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		 
		 createTableBySplitKeys("region_user_test",list);
		 
		 for (int i = 0; i < 100; i++) {
	        	if(i<10){
	        	//插入数据insertData----------->表名+配置+主键+列族
		            insertData("region_user_test", conf, "0"+String.valueOf(i), "tags");
	        	}else{
		            insertData("region_user_test", conf, String.valueOf(i), "tags");
	        	}
	        }
	}
	 
		public static void insertData(String tableName, Configuration configuration, String rowkey, String columnFamily) throws IOException {
	        System.out.println("start insert data ......");
	        HTable table = new HTable(configuration, "region_user_test");
	        Put put = new Put(rowkey.getBytes());
	        put.add(columnFamily.getBytes(), "family".getBytes(), "Java".getBytes());
	        try {
	            table.put(put);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        System.out.println("end insert data ......");
	    }

	 
	 
	public static byte[][] getSplitKeys(){
		String[] keys = new String[] { "10", "20", "30", "40", "50",  
                "60", "70", "80", "90" };  
        byte[][] splitKeys = new byte[keys.length][];  
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序  
        for (int i = 0; i < keys.length; i++) {  
            rows.add(Bytes.toBytes(keys[i]));  
        }  
        Iterator<byte[]> rowKeyIter = rows.iterator();  
        int i=0;  
        while (rowKeyIter.hasNext()) {  
            byte[] tempRow = rowKeyIter.next();  
            rowKeyIter.remove();  
            splitKeys[i] = tempRow;  
            i++;  
        }  
        return splitKeys;  
	}
	

	
	   public static boolean createTableBySplitKeys(String tableName, List<String> columnFamily) {    
	        try {    
	            HBaseAdmin admin = new HBaseAdmin(conf);    
	            if (admin.tableExists(tableName)) {    
	                return true;    
	            } else {    
	                HTableDescriptor tableDescriptor = new HTableDescriptor( TableName.valueOf(tableName));    
	                for (String cf : columnFamily) {    
	                    tableDescriptor.addFamily(new HColumnDescriptor(cf));    
	                }    
	                byte[][] splitKeys = getSplitKeys();    
	                admin.createTable(tableDescriptor,splitKeys);//指定splitkeys    
	                log.info("===Create Table " + tableName    
	                        + " Success!columnFamily:" + columnFamily.toString()    
	                        + "===");    
	            }    
	        } catch (MasterNotRunningException e) {    
	            // TODO Auto-generated catch block    
	            log.error(e);    
	            return false;    
	        } catch (ZooKeeperConnectionException e) {    
	            // TODO Auto-generated catch block    
	            log.error(e);    
	            return false;    
	        } catch (IOException e) {    
	            // TODO Auto-generated catch block    
	            log.error(e);    
	            return false;    
	        }    
	        return true;    
	    }  
}
