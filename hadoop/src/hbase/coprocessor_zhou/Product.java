package hbase.coprocessor_zhou;

public class Product {

	public String productid = null;		//	id为6位长度
	public String name = null;	//	产品名
	public Integer[] number = null;		//	产品数量
	public Product(String productid, String pname, Integer[] pnumber) {
		super();
		this.productid = productid;
		this.name = pname;
		this.number = pnumber;
	}
	public String getProductid() {
		return productid;
	}
	public void setProductid(String productid) {
		this.productid = productid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Integer[] getNumber() {
		return number;
	}
	public void setNumber(Integer[] number) {
		this.number = number;
	}
	
	
}
