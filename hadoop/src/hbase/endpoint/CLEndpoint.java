//package hbase.endpoint;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.hadoop.hbase.KeyValue;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
//import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
//import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
//import org.apache.hadoop.hbase.regionserver.HRegion;
//import org.apache.hadoop.hbase.regionserver.RegionScanner;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
//
//public class CLEndpoint extends BaseEndpointCoprocessor implements CLEPInterface
//{
//	private static Logger log;
//
//	public CLEndpoint()
//	{
//		PropertyConfigurator.configure("conf/cllog4j.properties");
//
//		log = Logger.getLogger(CLEndpoint.class);
//
//		//log.info("===================== Cloud Location End Point Starting =====================");
//	}
//
//	@Override
//	public Map<Integer, Long> queryNumByTime() throws IOException
//	{
//		RegionCoprocessorEnvironment environment = (RegionCoprocessorEnvironment) this.getEnvironment();
//		Map<Integer, Long> resu = new HashMap<Integer, Long>();
//
//		long starttime = System.currentTimeMillis();
//
//		//使用内部scanner做扫描
//		HRegion rg = environment.getRegion();
//		byte[] iRegionStart = getRegionNumStart(rg);
//		String strRegStart = Bytes.toString(iRegionStart);
//
//		Scan sc = getNewScan();
//		RegionScanner scanner = rg.getScanner(sc);
//
//		//计数 
//        try
//        {
//        	boolean hasMore=false; 
//            List<KeyValue> curValue = new ArrayList<KeyValue>();  
//            do{
//                curValue.clear();
//                hasMore=scanner.next(curValue);
//
//                if (!(curValue.isEmpty()))
//                {
//                	byte[] row = curValue.get(0).getRow();
//                	int irow = Bytes.toInt(row);	//	只是计数用,因此任意生成一个key
//                	
//                	addValue(resu, irow, 1);
//                }
//            }while(hasMore); 
//
//        }
//        catch(Exception e)
//        {
//        	log.error("queryNumByTime[" + strRegStart + "]: " + e.getStackTrace());
//        }
//        finally
//        {
//            if (null != scanner)
//            {
//            	scanner.close();
//            }
//
//            {
//    			long endtime = System.currentTimeMillis();
//    			long aaa = endtime - starttime;
//    			log.info("queryNumByTime[" + strRegStart + "] spends time: " + aaa );
//    		}
//        }
//
//		return resu;
//	}
//
//	@Override
//	public Map<Integer, String> queryParam() throws IOException
//	{
//		Map<Integer, String> resu = new HashMap<Integer, String>();
//		String str = System.getProperty("user.dir");
//		resu.put(0,  str);
//
//		return resu;
//	}
//
//	private void addValue(Map<Integer, Long> resu,int key, long value)
//	{
//		if (null == resu.get(key))
//		{
//			resu.put(key, value);
//		}
//		else
//		{
//			Long val = resu.get(key);
//			val += value;
//			resu.put(key, val);
//		}
//	}
//
//	private Scan getNewScan()
//	{
//		Scan sc = new Scan();
//		sc.addFamily(Const.m_tFamily);
//		sc.setFilter(new FirstKeyOnlyFilter());
//
//		sc.setMaxVersions(1);
//		
//		return sc;
//	}
//
//	//	获取region对应的编号
//	private byte[] getRegionNumStart(HRegion rg)
//	{
//		byte[] startKey = rg.getStartKey();
//
//		String strPre = "00";
//		byte[] iRegion = Bytes.toBytes(strPre);
//		if (startKey.length > 1)
//		{
//			String strTmp = Bytes.toString(startKey);
//			strTmp = strTmp.substring(0, 2);
//			iRegion = Bytes.toBytes(strTmp);
//		}
//
//		return iRegion;
//	}
//
//	public static void main(String [] args) throws Exception
//	{
//		CLEndpoint cl = new CLEndpoint();
//		cl.queryNumByTime();
//
//		System.exit(0);
//    }
//}