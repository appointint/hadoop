package hbase.coprocessor_zhou;

public class User {

	public String userid = null;		//	
	public String name = null;	//	
	public int age = -1;			//	
	public String[] productid;			//	
	public User(String userid, String name, int age, String[] l) {
		super();
		this.userid = userid;
		this.name = name;
		this.age = age;
		this.productid = l;
	}
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String[] getProductid() {
		return productid;
	}
	public void setProductid(String[] productid) {
		this.productid = productid;
	}
	
	
	
}
