package hbase.endpoint;

public class EPUtil
{
	public static String fullZero(String strOri, int iLength)
	{
		String strTotal = "";
		int iLen = iLength - strOri.length();
		for (int i = 0; i < iLen; i++)
		{
			strTotal += "0";
		}
		strTotal += strOri;
		
		return strTotal;
	}

	//将指定byte数组以16进制的形式打印到控制台
	public static String toHexStr( byte[] b)
	{
		if (null == b)
		{
			return null;
		}

		String str = "";
		for (int i = 0; i < b.length; i++)
		{
			String hex = Integer.toHexString(b[i] & 0xFF);
			if (hex.length() == 1)
			{
				hex = '0' + hex;
			}
			str +=hex.toUpperCase();
		}
		return str;
	}
}