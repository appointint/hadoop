package hbase.tablemore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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
import org.netlib.blas.Srot;

import shapeless.newtype;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class User_Book_Scan {
	/*
	 * 查询：先根据该用户点击的bookid查找用户id（ b-u 表），再根据用户id查找该用户看过的书籍id即bookid（u-b表）
	 */
	static Configuration cfg = HBaseConfiguration.create();

	static List<String> userlist = new ArrayList<String>();

	static List<String> booklist = new ArrayList<String>();

	
	public static void main(String[] args) throws Exception {

		System.out.println("bookid select userid");

		System.out.println("b_u table");
		User_Book_Scan.onescan("b_u", "003", "user");

		System.out.println("userlist:" + userlist.toString());

		System.out.println("userid select bookid");
		System.out.println("u_b  table");
		User_Book_Scan.twoscan("u_b","book");
		
		System.out.println("booklist:"+booklist.toString());

		System.out.println("001: " + Collections.frequency(booklist, "001"));
		List<String> resultlist=new ArrayList<String>();
		resultlist.addAll(booklist);
		System.out.println("resultlist:"+resultlist.toString());
		
		List<String> removelist=new ArrayList<String>();
		removelist=removeDuplicate(booklist);
		System.out.println("removelist："+removelist.toString());

		for(int i =0 ;i<removelist.size();i++){
			
			System.out.println(removelist.get(i)+" : "+Collections.frequency(resultlist,removelist.get(i)));
			 
			
		}
		

	}

	public   static   List  removeDuplicate(List list)  {       
		  for  ( int  i  =   0 ; i  <  list.size()  -   1 ; i ++ )  {       
		      for  ( int  j  =  list.size()  -   1 ; j  >  i; j -- )  {       
		           if  (list.get(j).equals(list.get(i)))  {       
		              list.remove(j);       
		            }        
		        }        
		      }        
		    return list;       
		}  
	@SuppressWarnings("deprecation")
	public static void onescan(String tablename, String row, String family) throws Exception {
		/*
		 * HTable table = new HTable(cfg, tablename); Scan s = new Scan();
		 * ResultScanner rs = table.getScanner(s); Integer condition=new
		 * Integer((int) (Math.random()*3+1)); String cd=condition.toString();
		 * 
		 * for (Result r : rs) { System.out.println("Scan:" + r); }
		 */

		HTable table = new HTable(cfg, tablename);
		// 执行函数
		Result result = table.getRowOrBefore(Bytes.toBytes(row), Bytes.toBytes(family));
		// 进行循环

		List<String> list = new ArrayList<String>();
		KeyValue[] keyValues = result.raw();
		
		for (KeyValue kv : keyValues) {

			System.out.println(Bytes.toString(kv.getRow()));
			System.out.println(Bytes.toString(kv.getFamily()));
			System.out.println(Bytes.toString(kv.getQualifier()));
			System.out.println(Bytes.toString(kv.getValue()));
			System.out.println(Bytes.toString(kv.getKey()));
			
			list.add(Bytes.toString(kv.getQualifier()));
			
			userlist.add(Bytes.toString(kv.getQualifier()));
			
		
			
		}
		System.out.println(list.toString());

		table.close();

	}

	@SuppressWarnings("deprecation")
	public static void twoscan(String tablename,String family) throws Exception {
		/*
		 * HTable table = new HTable(cfg, tablename); Scan s = new Scan();
		 * ResultScanner rs = table.getScanner(s); Integer condition=new
		 * Integer((int) (Math.random()*3+1)); String cd=condition.toString();
		 * 
		 * for (Result r : rs) { System.out.println("Scan:" + r); }
		 */

		HTable table = new HTable(cfg, tablename);

		
		
		for (int i=0;i<userlist.size();i++) {
			// 执行函数
			Result result = table.getRowOrBefore(Bytes.toBytes(userlist.get(i)), Bytes.toBytes(family));
			// 进行循环
			List<String> list = new ArrayList<String>();
			KeyValue[] keyValues = result.raw();
			for (KeyValue kv : keyValues) {
				System.out.println(Bytes.toString(kv.getRow()));
				System.out.println(Bytes.toString(kv.getFamily()));
				System.out.println(Bytes.toString(kv.getQualifier()));
				System.out.println(Bytes.toString(kv.getValue()));
				System.out.println(Bytes.toString(kv.getKey()));
				
				list.add(Bytes.toString(kv.getQualifier()));
				
				booklist.add(Bytes.toString(kv.getQualifier()));
				
			}
			System.out.println(list.toString());
			
			
		}
		table.close();

	}

}
