package hbase.baseconvert;


import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public class RegionPair  implements SplitAlgorithm{
	//日志显示
	 private static final Logger LOG = LoggerFactory.getLogger(RegionPair.class);
	 /*
	  * **************************实现预分区
	  */

	    public static void main(String[] args) throws IOException {
	        Configuration configuration = HBaseConfiguration.create();
	        configuration.set("hbase.zookeeper.quorum", "localhost");
	        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
	       String tablename="rt_user_tags_test";
			if (hBaseAdmin.tableExists(tablename)) {
				try {
					hBaseAdmin.disableTable(tablename);
					hBaseAdmin.deleteTable(tablename);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
	        //建表
	        HTableDescriptor tableDescriptor = new HTableDescriptor("rt_user_tags_test");
	        //添加列族到表中
	        tableDescriptor.addFamily(new HColumnDescriptor("tags"));
	        //拆分策略可重写byte[][] split,在SplitAlgorithm接口中
	        byte[][] splits = getHexSplits("0", "100", 10);
	        createTable(hBaseAdmin, tableDescriptor, splits);
	        for (int i = 0; i < 100; i++) {
	        	if(i<10){
	        	//插入数据insertData----------->表名+配置+主键+列族
		            insertData("rt_user_tags_test", configuration, (new Integer("000000000"+String.valueOf(i))).toHexString(new Integer("000000000"+String.valueOf(i))), "tags");
	        	}else{
		            insertData("rt_user_tags_test", configuration, "00000000"+String.valueOf(i), "tags");
	        	}
	        }
	    }
	    
	    
        /*
         * 建表
         * 创建表格时
         */
	    public static boolean createTable(HBaseAdmin hBaseAdmin, HTableDescriptor tableDescriptor, byte[][] splits) throws IOException {
	        try {
	            hBaseAdmin.createTable(tableDescriptor, splits);
	            return true;
	        } catch (Exception e) {
	            LOG.info("table" + tableDescriptor.getNameAsString() + "already exists!");
	            return false;
	        }
	    }
        /*
         * 设置startKey,endKey
         */
	    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
	        //startKey:001, endKey:100, 10regions[001, 010], [011, 020],...
	        byte[][] splits = new byte[numRegions - 1][];
	        BigInteger lowestKey = new BigInteger(startKey, 10);
	        BigInteger highestKey = new BigInteger(endKey, 10);
	        BigInteger rangge = highestKey.subtract(lowestKey);
	        BigInteger regionIncrement = rangge.divide(BigInteger.valueOf(numRegions));
	        lowestKey = lowestKey.add(regionIncrement);
	        for (int i = 0; i < numRegions - 1; i++) {
	            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
	            byte[] b = String.format("%010x", key).getBytes();
	            splits[i] = b;
	        }
	        return splits;
	    }
        /*
         * 插入数据
         */
	    public static void insertData(String tableName, Configuration configuration, String rowkey, String columnFamily) throws IOException {
	        System.out.println("start insert data ......");
	        HTable table = new HTable(configuration, "rt_user_tags_test");
	        Put put = new Put(rowkey.getBytes());
	        put.add(columnFamily.getBytes(), null, "Java".getBytes());
	        try {
	            table.put(put);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        System.out.println("end insert data ......");
	    }


		@Override
		public byte[] firstRow() {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public byte[] lastRow() {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public String rowToStr(byte[] arg0) {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public String separator() {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public void setFirstRow(String arg0) {
			// TODO Auto-generated method stub
			
		}


		@Override
		public void setFirstRow(byte[] arg0) {
			// TODO Auto-generated method stub
			
		}


		@Override
		public void setLastRow(String arg0) {
			// TODO Auto-generated method stub
			
		}


		@Override
		public void setLastRow(byte[] arg0) {
			// TODO Auto-generated method stub
			
		}


		@Override
		public byte[][] split(int arg0) {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public byte[] split(byte[] arg0, byte[] arg1) {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public byte[] strToRow(String arg0) {
			// TODO Auto-generated method stub
			return null;
		}

}

