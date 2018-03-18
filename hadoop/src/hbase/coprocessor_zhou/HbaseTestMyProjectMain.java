package hbase.coprocessor_zhou;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class HbaseTestMyProjectMain extends Thread {
	public static final String FOUR_NUM = "10000";
	public static final String SRI_NUM = "1000";
	public static final String TOW_NUM = "100";
	public static final String FOUR_GOODS = "20000";
	public static final String SRI_GOODS = "2000";
	public static final String TOW_GOODS = "200";
	public static List<User> ulist = new LinkedList<User>();
	public static List<Product> plist = new LinkedList<Product>();

	public static void main(String[] args) throws Exception {

		 UserAndProductOperationInterface Upoi=new UserAndProductOperation();
		 try {
//			 Upoi.UserProductNameFilter("kwf");
			 Upoi.ProductAllPut("ta");
		 } catch (IOException e1) {
		
		 e1.printStackTrace();
		 }

		/*
		 * 
		 * // 多线程插入数据User
		 * 
		 * Random random = new Random(); //
		 * =====================================================================
		 * =================================================
		 * 
		 * for(int i=0;i<100;i++){ String randomGoodsID=""; int
		 * Gid=random.nextInt(100);
		 * 
		 * if(Gid<10){
		 * 
		 * randomGoodsID="0"+Gid+SRI_GOODS;
		 * 
		 * }else if(Gid>=10&&Gid<100){
		 * 
		 * randomGoodsID=Gid+SRI_GOODS;
		 * 
		 * }else if(Gid>=100&&Gid<1000){
		 * 
		 * randomGoodsID=Gid+TOW_GOODS; }
		 * 
		 * //随机生成两个小写字母的名字缩写 String randomname=""; for(int
		 * index=0;index<2;index++){ int num=random.nextInt(27)+96; char
		 * str=(char)num; randomname=randomname+String.valueOf(str); }
		 * 
		 * //随机生成购买商品 List<Integer>usernumber=new ArrayList<Integer>();; for(int
		 * x=random.nextInt(4)+1;x>0;x--){
		 * 
		 * usernumber.add(1); } Integer [] array=new Integer[]{};; Integer
		 * []l=usernumber.toArray(array); plist.add(new
		 * Product(randomGoodsID,randomname,l)); }
		 * 
		 * //
		 * =====================================================================
		 * ================================================= for (int ROW_N = 0;
		 * ROW_N <= 100; ROW_N++) {
		 * 
		 * // 顺序生成rowkey:userid String rowkey = "";
		 * 
		 * if (ROW_N < 10) {
		 * 
		 * rowkey = ROW_N + FOUR_NUM;
		 * 
		 * } else if (ROW_N >= 10 && ROW_N < 100) {
		 * 
		 * rowkey = ROW_N + SRI_NUM;
		 * 
		 * } else if (ROW_N >= 100 && ROW_N < 1000) {
		 * 
		 * rowkey = ROW_N + TOW_NUM;
		 * 
		 * }
		 * 
		 * // 随机生成三个小写字母的名字缩写 String randomname = ""; for (int index = 0; index
		 * < 3; index++) { int num = random.nextInt(27) + 96; char str = (char)
		 * num; randomname = randomname + String.valueOf(str); }
		 * 
		 * // 随机生成年龄 int randomage = random.nextInt(4) + 18;
		 * 
		 * // 随机生成购买商品 List<String> ListGoodsID = new ArrayList<String>(); ; for
		 * (int x = random.nextInt(4) + 1; x > 0; x--) { String randomGoodsID =
		 * ""; int Gid = random.nextInt(100);
		 * 
		 * if (Gid < 10) {
		 * 
		 * randomGoodsID = FOUR_GOODS + Gid; ListGoodsID.add(randomGoodsID);
		 * 
		 * } else if (Gid >= 10 && Gid < 100) {
		 * 
		 * randomGoodsID = SRI_GOODS + Gid; if
		 * (!ListGoodsID.contains(randomGoodsID)) {
		 * ListGoodsID.add(randomGoodsID); }
		 * 
		 * } else if (Gid >= 100 && Gid < 1000) {
		 * 
		 * randomGoodsID = TOW_GOODS + Gid; if
		 * (!ListGoodsID.contains(randomGoodsID)) {
		 * ListGoodsID.add(randomGoodsID); } } } String[] array = new String[]
		 * {} ; String[] l = (String[]) ListGoodsID.toArray(array);
		 * ulist.add(new User(rowkey, randomname, randomage, l));
		 * 
		 * }
		 * 
		 * try { ThreadInsert(); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 * 
		 */		
		
	}

	public static void ThreadInsert() throws InterruptedException {
		System.out.println("---------开始多线程插入测试----------");
		long start = System.currentTimeMillis();
		int threadNumber = 5;
		Thread[] threads = new Thread[threadNumber];
		int startnum = 1;
		int endnum = 0;
		for (int i = 0; i < threads.length; i++) {
			startnum = endnum;
			endnum = (i + 1) * 100;
			threads[i] = new ImportThread(startnum, endnum);
			threads[i].start();

		}
		for (int j = 0; j < threads.length; j++) {
			(threads[j]).join();
		}
		long stop = System.currentTimeMillis();
		System.out.println("多线程插入共耗时：" + (stop - start) * 1.0 / 1000 + "s");
		System.out.println("---------多线程插入结束----------");
	}

	public static class ImportThread extends Thread {
		int startdata, enddata;

		public ImportThread(int startdata, int enddata) {
			this.startdata = startdata;
			this.enddata = enddata;
		}

		public void run() {
			try {
				UserAndProductOperationInterface Upoi = new UserAndProductOperation();
				 Upoi.MoreTheardInsertDataToProduct(plist);
				Upoi.MoreTheardInsertDataToUser(ulist);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				System.gc();
			}
		}
	}

}
