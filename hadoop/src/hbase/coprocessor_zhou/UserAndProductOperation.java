package hbase.coprocessor_zhou;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.dmg.pmml.Array;


public class UserAndProductOperation implements UserAndProductOperationInterface{
	
	
	public static final String USER="users";
    public static final String USER_COLUMN="userinfo";
    public static final String USER_NAME="name";
    public static final String USER_ID="userid";
    public static final String USER_AGE="age";
    public static final String USER_PRODUCTID="productid";
    public static final String PRODUCT="products";
    public static final String PRODUCT_NAME="name";
    public static final String PRODUCT_NUM="number";
    public static final String PRODUCT_COLUMN="productinfo";
    //Test
    public static HTablePool pool;
  //=======================================================================
    //=========================connecation and congriguration img============
 	/*
 	 * 连接池配置
 	 */
 	public static Configuration configuration;
 	/*
 	 * 连接池对象
 	 */
     public static Connection connection;
     private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
 	private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
     /*
 	 * Admin
 	 */
     public static Admin admin;
     /**
 	 * HBase位置
 	 */
 	@SuppressWarnings("unused")
 	private static final String HBASE_POS = "localhost";

 	/**
 	 * ZooKeeper位置
 	 */
 	private static final String ZK_POS = "localhost";

 	/**
 	 * zookeeper服务端口
 	 */
 	private final static String ZK_PORT_VALUE = "2181";
     /*
      * 建立连接
      */
    //==================================================================
    //=====================contry open and close========================
     public static void init(){
         configuration  = HBaseConfiguration.create();
//         configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
//         configuration.set(ZK_QUORUM, ZK_POS);
// 		configuration.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
         try{
             connection = ConnectionFactory.createConnection(configuration);
             //Test
             pool = new HTablePool(configuration, 1000); 
             admin = connection.getAdmin();
         }catch (IOException e){
             e.printStackTrace();
         }
     }
     /*
      * 关闭连接
      */
     public static void close(){
         try{
             if(admin != null){
                 admin.close();
             }
             if(null != connection){
                 connection.close();
             }
         }catch (IOException e){
             e.printStackTrace();
         }
     }
   //=======================================================================
   //===============================database operation======================
	@SuppressWarnings("deprecation")
	@Override
	public boolean MoreTheardInsertDataToUser(List<User>userlist) throws IOException {
			init();
			boolean IS_INSERT=false;
			long start = System.currentTimeMillis();
			//HTable table = (HTable)pool.getTable(USER);
	        /*
	         * HTable.setAutoFlush(false)方法可以将HTable写客户端的自动flush关闭，
	         * 这样可以批量写入数据到HBase，而不是有一条put就执行一次更新，
	         * 只有当put填满客户端写缓存时，才实际向HBase服务端发起写请求。默认情况下auto flush是开启的。
	         */
	        //table.setAutoFlush(false);
	        /*
	         * HTable.setWriteBufferSize(writeBufferSize)方法可以设置HTable客户端的写buffer大小,
	         * 如果新设置的buffer小于当前写buffer中的数据时，
	         * buffer将会被flush到服务端。其中，writeBufferSize的单位是byte字节数，可以根据实际写入数据量的多少来设置该值。
	         */
	        //table.setWriteBufferSize(24*1024*1024);
	        /*构造测试数据*/
	        List<Put> list = new ArrayList<Put>();
	        for(User user : userlist){
			        //向user插入 rowkey
			        Put put = new Put(user.getUserid().getBytes());
			        //向user插入 name
			        put.addColumn(USER_COLUMN.getBytes(), USER_NAME.getBytes(),
							user.getName().toString().getBytes());
			        
			        //向user插入 age
			        put.addColumn(USER_COLUMN.getBytes(), USER_AGE.getBytes(),
							String.valueOf(user.getAge()).getBytes());
			        
			        //向user插入 productid
			        List productidList=Arrays.asList(user.getProductid());
			        put.addColumn(USER_COLUMN.getBytes(), USER_PRODUCTID.getBytes(),
			        		productidList.toString().getBytes());
			        
			        /*
			         * 在HBae中，客户端向集群中的RegionServer提交数据时（Put/Delete操作），首先会先写WAL（Write Ahead Log）
			         * 日志（即HLog，一个RegionServer上的所有Region共享一个HLog），只有当WAL日志写成功后，再接着写MemStore，
			         * 然后客户端被通知提交数据成功；
			         * 如果写WAL日志失败，客户端则被通知提交失败。这样做的好处是可以做到RegionServer宕机后的数据恢复。
		             * 因此，对于相对不太重要的数据，可以在Put/Delete操作时，通过调用Put.setWriteToWAL(false)或Delete.
		             * setWriteToWAL(false)函数，放弃写WAL日志，从而提高数据写入的性能。
		             * 值得注意的是：谨慎选择关闭WAL日志，因为这样的话，一旦RegionServer宕机，Put/Delete的数据将会无法根据WAL日志进行恢复。
		             * 
			         */
		            put.setWriteToWAL(false);   
		            list.add(put);
	        }
	        if(userlist.size()==list.size()){
            //table.put(list);
	        //Test
	        pool.getTable(USER).put(list);
            list.clear();    
            //table.flushCommits();
            IS_INSERT=true;
	        }
	        long stop = System.currentTimeMillis();
	        System.out.println("线程:"+Thread.currentThread().getId()+"插入数据共耗时："+ (stop - start)*1.0/1000+"s");
	        pool.close();
	        return IS_INSERT;
			
	}
	
	
	@Override
	public boolean MoreTheardInsertDataToProduct(List<Product>productlist) throws IOException {
		init();
		boolean IS_INSERT=false;
		long start = System.currentTimeMillis();
        /*构造测试数据*/
        List<Put> list1 = new ArrayList<Put>();
        for(Product product : productlist){
		        //向product插入 rowkey
		        Put put = new Put(product.getProductid().getBytes());
		        //向product插入 name
		        put.addColumn(PRODUCT_COLUMN.getBytes(), PRODUCT_NAME.getBytes(),
		        		product.getName().toString().getBytes());
		        
		        //向product插入 number
		        List productidList1=Arrays.asList(product.getNumber());
		        put.addColumn(PRODUCT_COLUMN.getBytes(), PRODUCT_NUM.getBytes(),
		        		productidList1.toString().getBytes());
		
	            put.setWriteToWAL(false);   
	            list1.add(put);
        }
        if(productlist.size()==list1.size()){

	        pool.getTable(PRODUCT).put(list1);
	        list1.clear();    
	        IS_INSERT=true;
        }
        long stop = System.currentTimeMillis();
        System.out.println("线程:"+Thread.currentThread().getId()+"插入数据共耗时："+ (stop - start)*1.0/1000+"s");
        pool.close();
		return IS_INSERT;
	}
	
	
	
	@Override
	public void UserProductNameFilter(String username) throws IOException {
//		List<Filter>filters=new ArrayList<Filter>();
		init();
		Table table1= connection.getTable(TableName.valueOf(USER));
		Filter filter1=new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(username));
		Scan scan1=new Scan();
		scan1.setFilter(filter1);
		ResultScanner ss1 = table1.getScanner(scan1);  
		List<String>rowkeyList=new ArrayList<String>();
		List<String>productList=new ArrayList<String>();
		if(ss1!=null){
			System.out.println(username+"的row为:");
	        for(Result r:ss1){  
	        	for(KeyValue kv : r.raw()){  
	        		 System.out.println(kv);
	    		     //获取key
	    		     rowkeyList.add(new String(kv.getRow()));
	    		 }  
	        }
		}
		System.out.print(username+"的rowkey为：");
		System.out.println(rowkeyList); 
		
		for(String row :rowkeyList){
			Get get=new Get(Bytes.toBytes(row));
			get.addColumn(Bytes.toBytes(USER_COLUMN), Bytes.toBytes(USER_PRODUCTID));
			Result result=table1.get(get);
	        for(KeyValue kv : result.raw()){  
		        productList.add(new String(kv.getValue()));
	         }  
		}
		System.out.print("该用户购买的产品号为:");
		System.out.println(productList); 
        ss1.close();
        List<String>productname=new ArrayList<String>();
        List<String>splitarr=new ArrayList<String>();
        Table table2= connection.getTable(TableName.valueOf(PRODUCT));
        for(String pro :productList){
        	String[]arr=pro.split("[',''\\[''\\]']");
        	
        	for(String a : arr){
        		if(a.length()!=0){
        	      splitarr.add(a.trim());
        		}
        	}
        	System.out.println("产品号："+splitarr);
        	
        	for(String reals :splitarr){
        		String real=reals.substring(4,6)+"2000";
        		System.out.println("produid:"+real);
					Get get1=new Get(Bytes.toBytes(real));
					get1.addColumn(Bytes.toBytes(PRODUCT_COLUMN), Bytes.toBytes(PRODUCT_NAME));
					Result result1=table2.get(get1);
			        for(KeyValue kv1 : result1.raw()){  
			        	productname.add(new String(kv1.getValue()));
			         }  
        	}
		}
        System.out.print("用户购买的商品名为:");
        System.out.println(productname);
        
        
        System.out.print("请输入想要查询购买数量的产品:");
		Scanner s=new Scanner(System.in);
        String productn=s.next();
		Filter filter2=new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new SubstringComparator(productn));
		Scan scan2=new Scan();
		scan2.setFilter(filter2);
		ResultScanner ss2 = table2.getScanner(scan2); 
		List<String>rowkeyList1=new ArrayList<String>();
		if(ss2!=null){
			System.out.println("查询结果为:");
	        for(Result r:ss2){  
	        	for(KeyValue kv : r.raw()){  
	        		 System.out.println(kv);
	    		     //获取key
	    		     rowkeyList1.add(new String(kv.getRow()));
	    		 }  
	        }
		}
		System.out.print(productn+"的rowkey:");
		System.out.println(rowkeyList1); 
		

		List<String>numbers=new ArrayList<String>();
		for(String row :rowkeyList1){
			Get get2=new Get(Bytes.toBytes(row));
			get2.addColumn(Bytes.toBytes(PRODUCT_COLUMN), Bytes.toBytes(PRODUCT_NUM));
			Result result2=table2.get(get2);
	        for(KeyValue kv : result2.raw()){  
		        numbers.add(new String(kv.getValue()));
//	        	System.out.println(new String(kv.getValue()));
	         }  
		}
		
		List<Integer>  NUM=new ArrayList<Integer>();
		System.out.println("numbers产品数量:"+numbers); 
		Integer endnum=0;
		for(String pr :numbers){
			if(numbers.size()==1){
        	String[]arr1=pr.split("[',''\\[''\\]']");
//        	System.out.println("arr1:"+arr1.toString());
        	for(String a1 : arr1){
        		if(a1.length()!=0){
        	      NUM.add(Integer.valueOf(a1.trim()));
        			endnum=endnum+Integer.valueOf(a1.trim());
        		}
        	}
		
		}
		System.out.println("产品总数量："+endnum); 
		}
		
	}
	
	
	@SuppressWarnings({ "resource", "deprecation", "unused" })
	public void ProductAllPut(String productname)throws IOException{
		init();
		HTable ptable = new HTable(configuration, "products");
		HTable utable = new HTable(configuration, "users");
		String prokey = "";
//		String[] splits = new String[2];
//		String proids = "";

		List<String> plist = new ArrayList<String>();
		Scan scan = new Scan();
		ResultScanner resultScanner = utable.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println("rowkey:" + new String(result.getRow()));
			for (KeyValue keyValue : result.raw()) {
				System.out.println(
						"  列:" + new String(keyValue.getQualifier()) + ",值:" + new String(keyValue.getValue()));
				if (new String(keyValue.getQualifier()).equals("productid")) {
					String strInfo = new String(keyValue.getValue());
					System.out.println("utable proid:" + new String(keyValue.getValue()));
					String pro = new String(keyValue.getValue());
										
					String[]arr=pro.split("[',''\\[''\\]']");
		        	
		        	for(String a : arr){
		        		if(a.length()!=0){
		        			String real=a.trim().substring(4,6)+"2000";
		        	      plist.add(real.trim());
		        		}
		        	}				
				}
			}
		}
		System.out.println("plist:" + plist);
		resultScanner.close();

		
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(productname));
		Scan scan2 = new Scan();
		scan2.setFilter(filter);
		ResultScanner resultScanner2 = ptable.getScanner(scan2);
		for (Result result2 : resultScanner2) {
			prokey = new String(result2.getRow());
			System.out.println(productname+"的rowkey为:" + new String(result2.getRow()));
			for (KeyValue keyValue : result2.raw()) {
				System.out.println("  列限定符："+new String(keyValue.getFamily())+
						"    列:" + new String(keyValue.getQualifier()) + ",值:" + new String(keyValue.getValue()));
			}
		}

		System.out.println("prokey:"+prokey);
		
		String pronum="";
		Scan scan3=new Scan();
		Filter filter3=new RowFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(prokey)));
		scan3.setFilter(filter3);
		ResultScanner resultScanner3=ptable.getScanner(scan3);
		for (Result result : resultScanner3) {
			for (KeyValue keyValue : result.raw()) {
				System.out.println("  列限定符："+new String(keyValue.getFamily())+
						"    列:" + new String(keyValue.getQualifier()) + ",值:" + new String(keyValue.getValue()));
				if(new String(keyValue.getQualifier()).equals("number")){
					pronum=new String(keyValue.getValue());
				}
			}
		}
		resultScanner3.close();

		//prokey在plist中有多少个，共有多少个用户购买列prokey
        System.out.println("产品销量： " + Collections.frequency(plist, prokey));

		String[] numlist=pronum.split("[','' ''\\[''\\]']");
//		System.out.println(Arrays.asList(numlist));
		int allnum=0;
		for(String num:numlist){
			if(num.length()>0){
			int x=Integer.parseInt(num.trim());
//			System.out.println(num);
			allnum = allnum + x;
			}
		}
		
		System.out.println("总销量："+(allnum*Collections.frequency(plist, prokey)));
		
		
	}
	
	
	

}
