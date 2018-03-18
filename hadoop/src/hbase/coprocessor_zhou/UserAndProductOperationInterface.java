package hbase.coprocessor_zhou;

import java.io.IOException;
import java.util.List;


public interface UserAndProductOperationInterface {
	
	/**
	 * @param User : 为User表的实体类
	 * 向user表内插入数据
	 * @throws IOException 
	 */
	boolean MoreTheardInsertDataToUser(List<User>userlist) throws IOException;
	
	/**
	 * @param Product : 为product表的实体类
	 * 向product表内插入数据
	 * @throws IOException 
	 */
	boolean MoreTheardInsertDataToProduct(List<Product>productlist) throws IOException;
	
	
	void UserProductNameFilter(String username) throws IOException;
	
	 void ProductAllPut(String productname)throws IOException, Exception;

}
