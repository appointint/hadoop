package hbase.coprocessortest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;

import shapeless.syntax.singleton;



	@SuppressWarnings("deprecation")
	public class Tread_Insert {
		static Configuration hbaseConfig = null;
		public static HTablePool pool = null;
		public static String utableName = "users";
		static {
//			hbaseConfig=HBaseConfiguration.create();
			Configuration cfg = new Configuration();
			// cfg.set("hbase.master", "192.168.230.133:60000");
			// cfg.set("hbase.zookeeper.quorum", "192.168.230.133");
			// cfg.set("hbase.zookeeper.property.clientPort", "2181");
			hbaseConfig = HBaseConfiguration.create(cfg);
			pool = new HTablePool(hbaseConfig, 1000);
		}

		public static void main(String[] args) throws Exception {
			//SingleThreadInsert();
			 MultThreadInsert();
		}

		public static void SingleThreadInsert() throws IOException {
			System.out.println("---------开始SingleThreadInsert测试----------");
			long start = System.currentTimeMillis();
			// HTableInterface table = null;
			HTable table = null;
			table = (HTable) pool.getTable(utableName);
			table.setAutoFlush(false);
			table.setWriteBufferSize(24 * 1024 * 1024);
			// 构造测试数据
			List<Put> list = new ArrayList<Put>();
			int count = 100;
			byte[] buffer = new byte[350];
			Random rand = new Random();
			for (int i = 0; i < count; i++) {
				int[] prolist=new int[]{100200,101200,102200,112200};
				List<Integer> lsnum=new ArrayList<Integer>();
				for(int j =0;j<prolist.length;j++){
					int num=(int)Math.random()*4+1;
					lsnum.add(num);
				}
				HashSet h  =   new  HashSet(lsnum); 
			    lsnum.clear(); 
			    lsnum.addAll(h); 
			    String prostring="";
			    for (int j = 0; j < lsnum.size(); j++) {
			    	prostring=prostring+lsnum.get(j)+";";
				}
			    prostring=prostring.substring(0, prostring.length()-1);
				Put put = new Put(String.format("row%d", i).getBytes());
				rand.nextBytes(buffer);
				put.add("cf".getBytes(), null, buffer);
				// wal=false
				put.setWriteToWAL(false);
				list.add(put);
				if (i % 10000 == 0) {
					pool.getTable(utableName).put(list);
					list.clear();
					table.flushCommits();
				}
			}
			long stop = System.currentTimeMillis();
			// System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);
			System.out.println("插入数据：" + count + "共耗时：" + (stop - start) * 1.0 / 1000 + "s");
			System.out.println("---------结束SingleThreadInsert测试----------");
		}

		/*
		 * 多线程环境下线程插入函数
		 *
		 */
		@SuppressWarnings("deprecation")
		public static void InsertProcess() throws IOException {
			long start = System.currentTimeMillis();
			// HTableInterface table = null;
			HTable table = null;
			table = (HTable) pool.getTable(utableName);
			table.setAutoFlush(false);
			table.setWriteBufferSize(24 * 1024 * 1024);
			// 构造测试数据
			List<Put> list = new ArrayList<Put>();
			int count = 100;
			byte[] buffer = new byte[256];
			Random rand = new Random();
			for (int i = 0; i < count; i++) {
				Put put = new Put(String.format("row%d", i).getBytes());
				//向user插入 name
//		        put.addColumn(CLEntry.TUCOLUMN_N.getBytes(), CLEntry.TUNAME.getBytes(),
//						user.getName().toString().getBytes());
//		        
//		        //向user插入 age
//		        put.addColumn(USER_COLUMN.getBytes(), USER_AGE.getBytes(),
//						String.valueOf(user.getAge()).getBytes());
//		        
//		        //向user插入 productid
//		        List productidList=Arrays.asList(user.getProductid());
//		        put.addColumn(USER_COLUMN.getBytes(), USER_PRODUCTID.getBytes(),
//		        		productidList.toString().getBytes());
		        
		        
		        
				rand.nextBytes(buffer);
				put.add("cf".getBytes(), null, buffer);

				// wal=false
				put.setWriteToWAL(false);
				list.add(put);
				if (i % 10000 == 0) {
					pool.getTable(utableName).put(list);
					list.clear();
					table.flushCommits();
				}
			}
			long stop = System.currentTimeMillis();
			// System.out.println("WAL="+wal+",autoFlush="+autoFlush+",buffer="+writeBuffer+",count="+count);

			System.out.println(
					"线程:" + Thread.currentThread().getId() + "插入数据：" + count + "共耗时：" + (stop - start) * 1.0 / 1000 + "s");
		}

		/*
		 * Mutil thread insert test
		 */
		public static void MultThreadInsert() throws InterruptedException {
			System.out.println("---------开始MultThreadInsert测试----------");
			long start = System.currentTimeMillis();
			int threadNumber = 10;
			Thread[] threads = new Thread[threadNumber];
			for (int i = 0; i < threads.length; i++) {
				threads[i] = new ImportThread();
				threads[i].start();
			}
			for (int j = 0; j < threads.length; j++) {
				(threads[j]).join();
			}
			long stop = System.currentTimeMillis();

			System.out.println("MultThreadInsert：" + threadNumber * 10000 + "共耗时：" + (stop - start) * 1.0 / 1000 + "s");
			System.out.println("---------结束MultThreadInsert测试----------");
		}

		public static class ImportThread extends Thread {			
			public void run() {
				try {
					InsertProcess();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					System.gc();
				}
			}
		}
	}