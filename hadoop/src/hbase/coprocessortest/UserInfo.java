package hbase.coprocessortest;

public class UserInfo {

	public int userid = -1;		//	
	public String uname = null;	//	
	public int uage = -1;			//	
	public int[] uproductid;			//	
	public UserInfo(int userid, String name, int age, int[] uproductid) {
		super();
		this.userid = userid;
		this.uname = name;
		this.uage = age;
		this.uproductid = uproductid;
	}
	
	
	
}
