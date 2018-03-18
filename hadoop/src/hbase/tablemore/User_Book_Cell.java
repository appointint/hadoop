package hbase.tablemore;


	import java.io.IOException;
	import java.util.ArrayList;
	import java.util.HashSet;
	import java.util.Scanner;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.hbase.Cell;
	import org.apache.hadoop.hbase.CellUtil;
	import org.apache.hadoop.hbase.HBaseConfiguration;
	import org.apache.hadoop.hbase.client.Get;
	import org.apache.hadoop.hbase.client.HTable;
	import org.apache.hadoop.hbase.client.Result;
	import org.apache.hadoop.hbase.client.ResultScanner;
	import org.apache.hadoop.hbase.client.Scan;
	import org.apache.hadoop.hbase.util.Bytes;

	public class User_Book_Cell {

		static Configuration conf = HBaseConfiguration.create();
		public static void main(String[] args) throws Exception{
			
			Scanner sc=new Scanner(System.in );
			
			System.out.println("请输入bookid:");
			String bookid=sc.nextLine();
			if(bookid==null){
				System.out.println("该书不存在！");
			}else{
				getbook(bookid);
			}
			System.out.println("请输入userid:");
			String userid=sc.nextLine();
			if(userid==null){
				System.out.println("该读者不存在！");
			}else{
				getuser(userid);
			}
			
			//统计书本数和读者数
			System.out.println("请输入表名：");
			String tablename=sc.nextLine();
			if(tablename==null){
				System.out.println("该表不存在！");
			}else{
				countdata(tablename);
			}
			
		}
		
		//通过bookid(b_u)—》userid(b_u)—》bookid(u_b)
		public static void getbook(String bookid) throws IOException{
			HTable table=new HTable(conf,"b_u");
			HTable table2 = new HTable(conf,"u_b");
			Get get =new Get(Bytes.toBytes(bookid)); 
			Result r=table.get(get);
			//用HashSet去重
			HashSet set=new HashSet();
			int m=0;
			for(Cell c:r.raw()){
				m++;
				Get get2 =new Get(Bytes.toBytes(Bytes.toString(CellUtil.cloneQualifier(c)))); 
				Result r2=table2.get(get2);
				for(Cell c2:r2.raw()){
					set.add(Bytes.toString(CellUtil.cloneQualifier(c2)));
				}
			}
			System.out.println(bookid+"这本书被"+m+"个人读过。");
			System.out.println("这些人还读过其他的书:");
			for(Object name:set){
				System.out.println(name);
			}
		}
		
		//userid(u_b)—》bookid(u_b)—》userid(b_u)
		public static void getuser(String userid) throws IOException{
			HTable table=new HTable(conf,"u_b");
			HTable table2=new HTable(conf,"b_u");
			Get get =new Get(Bytes.toBytes(userid)); 
			Result r=table.get(get);
			int m=0;
			for(Cell c:r.raw()){
				m++;
				System.out.println("读过的书号："+Bytes.toString(CellUtil.cloneQualifier(c)));
				Get get2 =new Get(Bytes.toBytes(Bytes.toString(CellUtil.cloneQualifier(c)))); 
				Result r2=table2.get(get2);
				for(Cell c2:r2.raw()){
					System.out.println("读过这本书的其他读者："+Bytes.toString(CellUtil.cloneQualifier(c2)));
				}
			}
			System.out.println(userid+"一共读过"+m+"本书。");		
		}
		
		public static void countdata(String tablename) throws IOException{
			HTable table=new HTable(conf,tablename);
			Scan s=new Scan();
			ResultScanner rs = table.getScanner(s);
			int m=0;
			HashSet set=new HashSet();
			if(tablename.equals("b_u")){
				for (Result r : rs) {
					m++;
					for(Cell c:r.raw()){
						set.add(Bytes.toString(CellUtil.cloneRow(c)));
						//System.out.println("分别是："+Bytes.toString(CellUtil.cloneRow(c)));
					}
				}
				System.out.println("一共有"+m+"本书。");
				System.out.println("分别是：");
				for(Object name:set){
					System.out.println(name);
				}
			}else if(tablename.equals("u_b")){
				for (Result r : rs) {
					m++;
					for(Cell c:r.raw()){
						set.add(Bytes.toString(CellUtil.cloneRow(c)));
					}
				}
				System.out.println("一共有"+m+"个读者。");
				System.out.println("分别是：");
				for(Object name:set){
					System.out.println(name);
				}
			}
		}
		
	}

