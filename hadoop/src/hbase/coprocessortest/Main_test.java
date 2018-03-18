package hbase.coprocessortest;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import hbase.coprocessortest.UserInfo;

public class Main_test {
	public static Configuration conf = new HBaseConfiguration().create();

	@SuppressWarnings({ "static-access", "deprecation" })
	public static void main(String[] args) throws Exception {
		 HTable ptable = new HTable(conf, "product");
//		 HTable utable = new HTable(conf, "user");		
		 Op_Method op=new Op_Method();
		int[] prolist=new int[]{100200,101200,102200,112200};
		String[] uname=new String[]{"xifashui","xiaozao","yagao","pencil"};

//		int num=(int)((Math.random()+1)*100-1);
		//产品 
		for(int i=0;i<prolist.length;i++){
			ProductInfo pro1=new ProductInfo(prolist[i], uname[i], 1);
			ptable.put(op.mkPutProduct(pro1));
		}
//		ProductInfo pro2=new ProductInfo(prolist[1], uname[1], 55);
//		ptable.put(op.mkPutProduct(pro2));	
//		ProductInfo pro3=new ProductInfo(prolist[2], uname[2], 11);
//		ptable.put(op.mkPutProduct(pro3));	
//		ProductInfo pro4=new ProductInfo(prolist[3], uname[3], 8);
//		ptable.put(op.mkPutProduct(pro4));	
		
		//用户
		UserInfo u1=new UserInfo(100100,"zhangsan",22,prolist);		
		UserInfo u2=new UserInfo(100101,"lisi",24,prolist);
		UserInfo u3=new UserInfo(100102,"wangwu",22,prolist);	
		UserInfo u4=new UserInfo(100103,"zhaoliu",23,prolist);			
		op.mkPutUser(u1);
		op.mkPutUser(u2);
		op.mkPutUser(u3);
		op.mkPutUser(u4);
		
	}
	


}
