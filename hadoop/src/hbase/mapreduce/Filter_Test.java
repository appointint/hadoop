package hbase.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class Filter_Test {
	
	static Configuration conf = HBaseConfiguration.create();
	static String tableName="filtertest";
	
	public static void main(String[] args) throws Exception {
		
		create(tableName, "cf1","cf2");
		for (int i = 0; i < 100; i++) {
			put(tableName, "row-"+i, "cf1", "name","name"+ String.valueOf(i));
			put(tableName, "row-"+i, "cf1", "age", "age"+String.valueOf(i));
			put(tableName, "row-"+i, "cf2", "info", "info"+String.valueOf(i));
		}
//		rowFilter();
//		familyfilter();
//		qualifierfilter();
		filterList();
		
	}
		
	public static void create(String tablename, String columnFamily1,String columnFamily2) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tablename)) {
			System.out.println("table Exists!");
			System.out.println("正在删除");
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("删除成功");
		}
		System.out.println("正在创建。。。");
			@SuppressWarnings("deprecation")
			HTableDescriptor tableDesc = new HTableDescriptor(tablename);
			tableDesc.addFamily(new HColumnDescriptor(columnFamily1));
			tableDesc.addFamily(new HColumnDescriptor(columnFamily2));
			admin.createTable(tableDesc);
			System.out.println("create table success!");
		
	}

	public static void put(String tablename, String row, String columnFamily, String column, String data)
			throws IOException {
		HTable table = new HTable(conf, tablename);
		Put p1 = new Put(Bytes.toBytes(row));
		p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		table.put(p1);
		System.out.println("put'" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
	}
	
	@SuppressWarnings("deprecation")
	public static void rowFilter() throws IOException{
		HTable table=new HTable(conf, tableName);
		Scan scan=new Scan();
		System.out.println("scan table1..........................................................");
		Filter filter=new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes("row-22")));
		scan.setFilter(filter);
		ResultScanner resultScanner=table.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println(result);
		}
		resultScanner.close();
		System.out.println("scan table2 ...........................................................");
		Filter filter2=new RowFilter(CompareFilter.CompareOp.EQUAL,
				new RegexStringComparator((".*-.5")));
		scan.setFilter(filter2);
		ResultScanner resultScanner2=table.getScanner(scan);
		for (Result result : resultScanner2) {
			System.out.println(result);
		}
		resultScanner2.close();
		System.out.println("scan table3 ...............................................................");
		Filter filter3=new RowFilter(CompareFilter.CompareOp.EQUAL, 
				new SubstringComparator(("-5")));
		scan.setFilter(filter3);
		ResultScanner resultScanner3=table.getScanner(scan);
		for (Result result : resultScanner3) {
			System.out.println(result);
		}
		resultScanner3.close();
	}
	
	@SuppressWarnings("deprecation")
	public static void familyfilter()throws IOException{
		HTable table=new HTable(conf, tableName);
		Filter filter1=new FamilyFilter(CompareFilter.CompareOp.LESS, 
				new BinaryComparator(Bytes.toBytes("cf2")));
		Scan scan=new Scan();
		scan.setFilter(filter1);
		ResultScanner resultScanner=table.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println(result);
		}
		resultScanner.close();
		Get get=new Get(Bytes.toBytes("row-5"));
		get.setFilter(filter1);
		Result result=table.get(get);
		System.out.println("result:"+result);
		
		Filter filter2=new FamilyFilter(CompareFilter.CompareOp.EQUAL, 
				new BinaryComparator(Bytes.toBytes("cf2")));
		Get get2=new Get(Bytes.toBytes("row-5"));
		get2.setFilter(filter2);
		Result result2=table.get(get2);
		System.out.println("result2:"+result2);
	}
	
	@SuppressWarnings("deprecation")
	public static void qualifierfilter() throws IOException{
		HTable table=new HTable(conf, tableName);
		Filter filter=new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, 
				new BinaryComparator(Bytes.toBytes("name")));
		Scan scan=new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner=table.getScanner(scan);
		for (Result result : resultScanner) {
			System.out.println("qualify  result:"+result);
		}
		resultScanner.close();
		
		Get get=new Get(Bytes.toBytes("row-5"));
		get.setFilter(filter);
		Result result=table.get(get);
		for (KeyValue res : result.raw()) {
			System.out.println("KV:"+res+",value:"+Bytes.toString(res.getValue()));
		}
		
	}
	
	public static void filterList() throws IOException{
		HTable table=new HTable(conf, tableName);
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
		new BinaryComparator(Bytes.toBytes("row-90")));
		filters.add(filter1);

		Filter filter2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
		new BinaryComparator(Bytes.toBytes("row-95")));
		filters.add(filter2);

		Filter filter3  = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
		new SubstringComparator("name"));
		filters.add(filter3);

		FilterList filterList1 = new FilterList(filters);

		System.out.println("filterList1*****MUST_PASS_ALL*******************************************************");
		Scan scan = new Scan();
		scan.setFilter(filterList1);
		ResultScanner scanner1 = table.getScanner(scan);
		for(Result res : scanner1){
		for(KeyValue kv: res.raw())
		System.out.println("KV1: "+kv+",value: "+Bytes.toString(kv.getValue()));
		}
		scanner1.close();

		System.out.println("filterList2********MUST_PASS_ONE****************************************************");
		//第二个扫描器中设置了MUST_PASS_ONE，表示只要数据通过了一个过滤器的过滤就返回
		FilterList filterList2 = new FilterList(FilterList.Operator.MUST_PASS_ONE,filters);
		scan.setFilter(filterList2);
		ResultScanner scanner2 = table.getScanner(scan);
		for(Result res : scanner2){
		for(KeyValue kv: res.raw())
		System.out.println("KV2: "+kv+",value: "+Bytes.toString(kv.getValue()));
		}
		scanner2.close();
		}
	
}
