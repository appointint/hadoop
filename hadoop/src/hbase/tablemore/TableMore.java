//package hbase.tablemore;
//
//public class TableMore {
//
//}
//	// =========================================组合键查询时间段的数据
//	/*
//	 * 1.创建表access_log 2.插入数据 3.实现查询
//	 */
//	// 建表
//	// Himp.createTable("access_log", fimaly);
//	// 插入
//	// for(int i=0;i<100;i++){
//	// long time=System.currentTimeMillis();
//	// String newtime=String.valueOf(time);
//	// //System.out.println(time);
//	// if(i<100&&i>=10){
//	// Himp.putData("access_log", newtime+"00"+i,"http", "ip", "192.168.0."+i);
//	// Himp.putData("access_log", newtime+"00"+i,"http", "domain", i-1+"~"+i);
//	// Himp.putData("access_log", newtime+"00"+i,"http", "url",
//	// "www."+i+".com");
//	// Himp.putData("access_log", newtime+"00"+i,"http", "referer",
//	// "Alibaba"+i);
//	//
//	// Himp.putData("access_log", newtime+"00"+i,"user", "browser_cookie",
//	// "%@4235ab4b"+i);
//	// Himp.putData("access_log", newtime+"00"+i,"user", "login_id", "00"+i);
//	// }else {
//	// Himp.putData("access_log", newtime+"000"+i,"http", "ip", "192.168.0."+i);
//	// Himp.putData("access_log", newtime+"000"+i,"http", "domian", i-1+"~"+i);
//	// Himp.putData("access_log", newtime+"000"+i,"http", "url",
//	// "www."+i+".com");
//	// Himp.putData("access_log", newtime+"000"+i,"http", "referer",
//	// "Alibaba"+i);
//	//
//	// Himp.putData("access_log", newtime+"000"+i,"user", "browser_cookie",
//	// "%@4235ab4b"+i);
//	// Himp.putData("access_log", newtime+"000"+i,"user", "login_id", "000"+i);
//	// }
//	//
//	// }
//	// 查询
//
//	// Date d = new Date(LONG_AT_THE_EARLIEST_TIME);
//	// Calendar c = Calendar.getInstance();
//	// c.setTime(d);
//	// //此处设置查询时间段
//	// c.add(Calendar.SECOND, 10);
//	// String endtime=String.valueOf(c.getTimeInMillis());
//	// System.out.println(c.getTimeInMillis());
//	// Himp.GetDateFromCombinationKey("access_log",
//	// AT_THE_EARLIEST_TIME,endtime);
//
//	// ============================================================================================
//
//	/**
//	 * @param tablename
//	 * @param strrowkey
//	 * @param endrowkey
//	 *            暂且查看一分钟的吧
//	 */
//	@Override
//	public void GetDateFromCombinationKey(String tablename, String strrowkey, String endrowkey) throws IOException {
//		init();
//		Table table = connection.getTable(TableName.valueOf(tablename));
//		Scan scan = new Scan();
//		scan.setStartRow(Bytes.toBytes(strrowkey));
//		scan.setStopRow(Bytes.toBytes(endrowkey));
//		ResultScanner rs = table.getScanner(scan);
//		for (Result r : rs) {
//			for (Cell cell : r.rawCells()) {
//				// if(strrowkey.equals(new
//				// String(CellUtil.cloneRow(cell)).substring(0, 13))){
//				System.out.print(" Rowkey: " + new String(CellUtil.cloneRow(cell)));
//				System.out.print(" column: " + new String(CellUtil.cloneFamily(cell)));
//				System.out.print(" :qualifier: " + new String(CellUtil.cloneQualifier(cell)));
//				System.out.print(" value: " + new String(CellUtil.cloneValue(cell)));
//				System.out.println(" timestamp: " + cell.getTimestamp());
//				// }
//
//			}
//		}
//		table.close();// 释放资源
//	}
//
//}
//
//	// =======================================================================================================
//
///**
//* 位置案例,输出一个地方的上级位置和下级位置
//* @param tablename 
//     表名
//* @param spacename 
//     需要查询的位置
//*/
//@SuppressWarnings("deprecation")
//@Override
//public void GetDateFromSpaceNeed(String tablename, String sapcename) {
//init();
//Table table = null;
////创建一个list用来装student_course_table表中一行row所对应的value(其实就是获取列限定符)
//List<String> parentplace = new ArrayList<String>();
////用一个list容器来装推荐的书籍(不重复输出)
//Set<String> childplace = new HashSet<String>();
//List<String> cityname = new ArrayList<String>();
//List<String> childcityname = new ArrayList<String>();
//String rowkey="";
//switch(sapcename){
//case "chengdu":
//	rowkey="5";
//}
//
//try {
//	if(tablename!=null){
//	table = connection.getTable(TableName.valueOf(tablename));
//	
//
//	
//	/************************************************************************/
//         Get get = new Get(Bytes.toBytes(rowkey));
//         get.addFamily(Bytes.toBytes("child"));
//         Result res = table.get(get);
//         KeyValue[] kvs=res.raw();  
//            for(KeyValue kv:kvs)  
//            {  
//            	
//            	childplace.add(Bytes.toString(kv.getQualifier()));
//            	//System.out.println(Bytes.toString(kv.getQualifier())+"->"+Bytes.toString(kv.getValue()));
//	        	      
//            }  
//
//System.out.println("下级城市代号:"+childplace);
//
//
//
///******************************************************/
//for(String gainuserid1 : childplace){
// Get get3 = new Get(Bytes.toBytes(gainuserid1));
// get3.addFamily(Bytes.toBytes(" "));
// Result res3 = table.get(get3);
// KeyValue[] kvs3=res3.raw();  
//    for(KeyValue kv3:kvs3)  
//    {  
//    	if(!rowkey.equals(Bytes.toString(kv3.getFamily()))){
//    		 childcityname.add(Bytes.toString(kv3.getValue()));
//    	      
//    	}
//    }  
//
//}
//
//System.out.println("下级城市名字:"+ childcityname );
////table.close();
///********************************************************************/
////table = connection.getTable(TableName.valueOf(tablename));
////====================================================
//Get get1 = new Get(Bytes.toBytes(rowkey));
// get1.addFamily(Bytes.toBytes("parent"));
// Result res1 = table.get(get1);
// KeyValue[] kvs1=res1.raw();  
//    for(KeyValue kv1:kvs1)  
//    {  
//    	if(!" ".equals(Bytes.toString(kv1.getQualifier()))){
//    	parentplace.add(Bytes.toString(kv1.getQualifier()));
//    	//System.out.println(Bytes.toString(kv1.getQualifier())+"->"+Bytes.toString(kv1.getValue()));
//    	}
//    	      
//    }  
//
//System.out.println("上级城市代号:"+parentplace);
///***********************************************************************/
//for(String gainuserid2 : parentplace){
//
//System.out.println("parent result is ->  "+gainuserid2);
//Get get0 = new Get(Bytes.toBytes(gainuserid2));
//System.out.println("get0 ->  "+get0);
//get0.addFamily(Bytes.toBytes(" "));
//Result res0 = table.get(get0);
//KeyValue[] kvs0=res0.raw();  
//for(KeyValue kv00:kvs0)  
//{  
//          		cityname.add(Bytes.toString(kv00.getValue()));
//                // System.out.println("测试结果 :"+Bytes.toString(kv0.getValue()));
//}  
//
//}
//
//System.out.println("上级城市名字:"+cityname);
//
////======================================================================
//
//table.close();
//
//}
//} catch (IOException e) {
//	e.printStackTrace();
//}
//
////try {
////	table.close();
////} catch (IOException e) {
////	e.printStackTrace();
////}
//
//
//}
