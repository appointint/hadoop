package hbase.coprocessortest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Op_Method {

	public static Configuration conf = new HBaseConfiguration().create();

	@SuppressWarnings("deprecation")
	public static Put mkPutProduct(ProductInfo pro) throws Exception {
//		 HTable ptable = new HTable(conf, "product");
		Put p = new Put(Bytes.toBytes(String.valueOf(pro.productid)));
		p.add(Bytes.toBytes(CLEntry.TPFAMILY), Bytes.toBytes(CLEntry.TPNAME), Bytes.toBytes(pro.pname));
		String num =Integer.toString(pro.pnumber);
		p.add(Bytes.toBytes(CLEntry.TPFAMILY), Bytes.toBytes(CLEntry.TPCOLUMN_NUM), Bytes.toBytes(num));
		System.out.println("product put  : id="+pro.productid+",name="+pro.pname+",pnum="+pro.pnumber);
//		ptable.put(p);
		return p;
	}
	@SuppressWarnings("deprecation")
	public static void mkPutUser(UserInfo user) throws Exception {
		 HTable utable = new HTable(conf, "user");
		Put p = new Put(mkKey(user.userid));
		p.add(Bytes.toBytes(CLEntry.TUFAMILY), Bytes.toBytes(CLEntry.TUCOLUMN_N), Bytes.toBytes(user.uname));
		String strAge = Integer.toString(user.uage);
		p.add(Bytes.toBytes(CLEntry.TUFAMILY), Bytes.toBytes(CLEntry.TUCOLUMN_A), Bytes.toBytes(strAge));
		String strPro = makePro(user.uproductid);
		p.add(Bytes.toBytes(CLEntry.TUFAMILY), Bytes.toBytes(CLEntry.TUCOLUMN_P), Bytes.toBytes(strPro));
		utable.put(p);
	}
	

	@SuppressWarnings("deprecation")
	private static String makePro(int[] uproductid) throws IOException {
		List<Integer>  pList2=new ArrayList<Integer>();
		HTable table=new HTable(conf, "product");
        try { 
        	Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan); 
            int i=0;
            for (Result r : rs) { 
                System.out.println("获得到rowkey:" + new String(r.getRow())); 

                String strInfo = new String(r.getRow());
        		String strKey = strInfo.substring(strInfo.length() - 2, strInfo.length());
        		strKey += strInfo.substring(0, strInfo.length() - 2);

        		int v =Integer.parseInt(new String (strKey));
                pList2.add(Integer.parseInt(new String(r.getRow())));
                
                for (KeyValue keyValue : r.raw()) { 
                    System.out.println("列：" + new String(keyValue.getFamily()) 
                            + "====值:" + new String(keyValue.getValue())); 
                } 
            } 
        } catch (IOException e) { 
            e.printStackTrace(); 
        } 
        System.out.println("pro rowkey list"+pList2.toString());
        int[]  res=new int[2];
        for(int i=0;i<2;i++){
        	int index=(int)(Math.random()*pList2.size());
        	res[i]=pList2.get(index);
        }
        String results="";
		
        for (int i = 0; i < res.length; i++) {
        	String strInfo = Integer.toString(res[i]);
    		String strKey = strInfo.substring(strInfo.length() - 3, strInfo.length());
    		strKey += strInfo.substring(0, strInfo.length() - 3);
    		results=results+strKey+";";
		}
        System.out.println("value:"+results);
		return results.substring(0, results.length()-1);
	}

	public static byte[] mkKey(int iId) {
		String strInfo = Integer.toString(iId);
		String strKey = strInfo.substring(strInfo.length() - 3, strInfo.length());
		strKey += strInfo.substring(0, strInfo.length() - 3);

		byte[] rowKey = Bytes.toBytes(strKey);
		return rowKey;
	}

	@SuppressWarnings({ "deprecation", "resource" })
	public static void createTable(String tableName, String[] family) {
		System.out.println("start create table ......");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for (String fl : family) {
				tableDescriptor.addFamily(new HColumnDescriptor(fl));
			}
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

}
