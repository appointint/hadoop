package hbase.coprocessortest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/*
 * 使用filter 查询：name为张三的用户 购买了产品name为 海飞丝的洗发水的 数量（number）
 *使用filter 查询：产品name为中华牙膏 的 总销量
 */
public class FilterTable {
	static Configuration conf = HBaseConfiguration.create();
	static String ptableName = "product";
	static String utableName = "user";

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		// filter1("zhangsan");

		filter2("yagao");

	}

	// name为张三的用户 购买了产品name为 海飞丝的洗发水的 数量（number）
	public static void filter1(String name) throws IOException {
		HTable ptable = new HTable(conf, ptableName);
		HTable utable = new HTable(conf, utableName);
		String userkey = "";
		String[] splits = new String[2];
		String proids = "";
		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(name));

		Scan scan = new Scan();
		scan.setFilter(filter);
		ResultScanner resultScanner = utable.getScanner(scan);
		for (Result result : resultScanner) {
			userkey = new String(result.getRow());
			System.out.println("rowkey:" + new String(result.getRow()));
			for (KeyValue keyValue : result.raw()) {
				System.out.println("  zhangsan    列:" + new String(keyValue.getFamily()) + ",值:"
						+ new String(keyValue.getValue()));
			}
		}
		resultScanner.close();

		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(userkey)));
		scan.setFilter(filter2);
		ResultScanner resultScanner2 = utable.getScanner(scan);
		for (Result result : resultScanner2) {
			System.out.println("rowkey:" + new String(result.getRow()));

			for (KeyValue keyValue : result.raw()) {
				System.out.println("      列:" + new String(keyValue.getFamily()) + "列限定符："
						+ new String(keyValue.getQualifier()) + ",值:" + new String(keyValue.getValue()));
				if (new String(keyValue.getQualifier()).equals("productid")) {

					String strInfo = new String(keyValue.getValue());
					System.out.println("utable proid:" + new String(keyValue.getValue()));
					String pro = new String(keyValue.getValue());
					System.out.println("pro" + pro);
					String[] sps = pro.split(";");
					System.out.println("sps:" + sps[0] + "," + sps[1]);
					for (int i = 0; i < sps.length; i++) {

						String strKey = sps[i].substring(sps[i].length() - 3, sps[i].length());
						strKey += sps[i].substring(0, sps[i].length() - 3);
						splits[i] = strKey;

						System.out.println("proids:" + strKey);
					}
					System.out.println("splits:" + splits.toString());
				}
			}
		}
		resultScanner2.close();

		System.out.println("产品有：" + splits[0] + "," + splits[1]);

		boolean flag = true;
		while (flag) {
			System.out.println("请输入购买的产品号");
			Scanner sc = new Scanner(System.in);
			String pronum = sc.nextLine();
			for (int i = 0; i < splits.length; i++) {
				if (splits[i].equals(pronum)) {
					Filter filter3 = new RowFilter(CompareFilter.CompareOp.EQUAL,
							new BinaryComparator(Bytes.toBytes(pronum)));
					scan.setFilter(filter3);
					ResultScanner resultScanner3 = ptable.getScanner(scan);
					for (Result result : resultScanner3) {
						System.out.println("rowkey:" + new String(result.getRow()));
						for (KeyValue keyValue : result.raw()) {
							// System.out.println(" 列:" + new
							// String(keyValue.getFamily()) + "列限定符："
							// + new String(keyValue.getQualifier()) + ",值:" +
							// new String(keyValue.getValue()));
							if (new String(keyValue.getQualifier()).equals("product")) {
								System.out.println(splits[i] + "产品：" + new String(keyValue.getValue()));

							}
							if (new String(keyValue.getQualifier()).equals("number")) {
								System.out.println(splits[i] + " 数量:" + new String(keyValue.getValue()));

							}
							flag = false;
						}
					}
					resultScanner3.close();
				}
			}
		}

	}

	// 产品name为中华牙膏 的 总销量
	public static void filter2(String pname) throws IOException {
		HTable ptable = new HTable(conf, ptableName);
		HTable utable = new HTable(conf, utableName);
		String prokey = "";
		String[] splits = new String[2];
		String proids = "";

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
					// System.out.println("pro：" + pro);
					String[] sps = pro.split(";");
					for (int i = 0; i < sps.length; i++) {
						String strKey = sps[i].substring(sps[i].length() - 3, sps[i].length());
						strKey += sps[i].substring(0, sps[i].length() - 3);
						splits[i] = strKey;
						plist.add(strKey);
					}
				}
			}
		}
		System.out.println("plist:" + plist);
		resultScanner.close();

		Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(pname));
		Scan scan2 = new Scan();
		scan2.setFilter(filter);
		ResultScanner resultScanner2 = ptable.getScanner(scan2);
		for (Result result : resultScanner2) {
			prokey = new String(result.getRow());
			System.out.println("pro rowkey:" + new String(result.getRow()));
			for (KeyValue keyValue : result.raw()) {
				System.out.println(
						"    列:" + new String(keyValue.getQualifier()) + ",值:" + new String(keyValue.getValue()));
			}
		}
		resultScanner.close();

		String num="";
		Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(prokey)));
		scan.setFilter(filter2);
		ResultScanner resultScanner3 = ptable.getScanner(scan);
		for (Result result : resultScanner3) {
			System.out.println("rowkey:" + new String(result.getRow()));

			for (KeyValue keyValue : result.raw()) {
				System.out.println(
						"   列限定符：" + new String(keyValue.getQualifier()) + ",值:" + new String(keyValue.getValue()));
				if (new String(keyValue.getQualifier()).equals("number")) {
					num=new String(keyValue.getValue());
				}
			}
		}
		System.out.println("产品数量："+num);


        System.out.println("产品销量： " + Collections.frequency(plist, prokey));

		
		
	}
	

}
