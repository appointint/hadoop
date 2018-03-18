package hbase.baseconvert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Treadmore2 {

	static Configuration conf = HBaseConfiguration.create();
	@SuppressWarnings("deprecation")
	public static HTablePool pool = new HTablePool(conf, 1000);
	public static String tableName = "threadmore";

	@SuppressWarnings("deprecation")
	public static void InsertProcess(int startdata, int enddata) throws IOException {
		long start = System.currentTimeMillis();		
		HTableInterface table = pool.getTable(tableName);
		table.setAutoFlush(false);
		table.setWriteBufferSize(24 * 1024 * 1024);
		// 构造测试数据
		List<Put> list = new ArrayList<Put>();
		int count = 100;
		for (int i = startdata; i < enddata; i++) {
			Put put = new Put(String.format("row %d", i).getBytes());
			put.add("cf".getBytes(), null, Bytes.toBytes("" + i));
			put.setWriteToWAL(false);
			list.add(put);
			if (i % 10000 != 0) {
				table.put(list);
				list.clear();
				table.flushCommits();
			}
		}
		long stop = System.currentTimeMillis();
		System.out.println(
				"线程:" + Thread.currentThread().getId() + "插入数据：" + count + "共耗时：" + (stop - start) * 1.0 / 1000 + "s");
	}

	public static void ThreadInsert() throws InterruptedException {
		System.out.println("---------开始MultThreadInsert测试----------");
		long start = System.currentTimeMillis();
		int threadNumber = 10;
		Thread[] threads = new Thread[threadNumber];
		int startnum = 1;
		int endnum = 0;
		for (int i = 0; i < threads.length; i++) {
			startnum = endnum ;
			endnum = (i + 1) * 100;
			threads[i] = new ImportThread(startnum, endnum);
			threads[i].start();
			
		}
		for (int j = 0; j < threads.length; j++) {
			(threads[j]).join();
		}
		long stop = System.currentTimeMillis();

		System.out.println("MultThreadInsert：" + threadNumber * 100 + "共耗时：" + (stop - start) * 1.0 / 1000 + "s");
		System.out.println("---------结束MultThreadInsert测试----------");
	}

	public static class ImportThread extends Thread {
		int startdata, enddata;

		public ImportThread(int startdata, int enddata) {
			this.startdata = startdata;
			this.enddata = enddata;
		}

		public void run() {
			try {
				InsertProcess(startdata, enddata);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				System.gc();
			}
		}
	}
	@SuppressWarnings({ "deprecation", "resource" })
	public static void createTable(String tableName,String family1) {  
        System.out.println("start create table ......");  
        try {  
        	HBaseAdmin hBaseAdmin = new HBaseAdmin(conf); 
            if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  
                hBaseAdmin.disableTable(tableName);  
                hBaseAdmin.deleteTable(tableName);  
                System.out.println(tableName + " is exist,detele....");  
            }  
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
            tableDescriptor.addFamily(new HColumnDescriptor(family1));  
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
	public static void main(String[] args) throws InterruptedException, MasterNotRunningException, ZooKeeperConnectionException, IOException {
		
		
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf); 
        if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建  
            hBaseAdmin.disableTable(tableName);  
            hBaseAdmin.deleteTable(tableName);  
            System.out.println(tableName + " is exist,detele....");  
        }  
        
        createTable(tableName, "cf");
        
		ThreadInsert();
	}

}
