package hbase.baseconvert;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.ZooKeeperConnectionException;  
import org.apache.hadoop.hbase.client.Delete;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.HTablePool;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.filter.Filter;  
import org.apache.hadoop.hbase.filter.FilterList;  
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;  
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;  
import org.apache.hadoop.hbase.util.Bytes; 
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class User_Book_Scan {
	
	static Configuration cfg = HBaseConfiguration.create();

	public static void main(String[] args) throws Exception {
		System.out.println("u_b  table");
		User_Book_Scan.scan("u_b");
		
		System.out.println("b_u table");
		User_Book_Scan.scan("b_u");
		
		System.out.println("bookid select userid");
		
		
		
		System.out.println("userid select bookid");
		

	}
	
    public static void QueryByCondition2(String tableName) {  
    	  
        try {  
        	String cfString=null;
        	String family=null;
        	
            HTablePool pool = new HTablePool(cfg, 1000);  
            HTable table = (HTable) pool.getTable(tableName);
            if(tableName=="u_b"){
            	
            }
            Filter filter = new SingleColumnValueFilter(Bytes  
                    .toBytes("column1"), null, CompareOp.EQUAL, Bytes  
                    .toBytes("aaa")); // 当列column1的值为aaa时进行查询  
            Scan s = new Scan();  
            s.setFilter(filter);  
            ResultScanner rs = table.getScanner(s);  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("列：" + new String(keyValue.getFamily())  
                    		+"===列限定符"+new String(keyValue.getQualifier())
                            + "====值:" + new String(keyValue.getValue()));  
                }  
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
  
    }  
	
	public static void scan(String tablename) throws Exception {
		HTable table = new HTable(cfg, tablename);
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		Integer condition=new Integer((int) (Math.random()*3+1));
		String cd=condition.toString();
		
		for (Result r : rs) {
			System.out.println("Scan:" + r);
		}
	}

}
