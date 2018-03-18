package hbase.coprocessortest;

public class ProductInfo {

	public int productid = -1;		//	id为6位长度
	public String pname = null;	//	产品名
	public int pnumber = -1;		//	产品数量
	public ProductInfo(int productid, String pname, int pnumber) {
		super();
		this.productid = productid;
		this.pname = pname;
		this.pnumber = pnumber;
	}
	
	
}
