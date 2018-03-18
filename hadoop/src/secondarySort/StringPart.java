package secondarySort;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public   class StringPart implements WritableComparable<StringPart>{
	private String first;
	private String second;
	public String getFirst(){
		return first;
	}
	public void setFirst(String first){
		this.first=first;
	}
public String getSecond(){
		return second;
	}
	public void setSecond(String second){
		this.second=second;
	}
	public StringPart(){
		super();
	}
	public StringPart(String first,String second){
		super();
		this.first=first;
		this.second=second;
	}
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeUTF(first);
		out.writeUTF(second);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException{
		this.first=in.readUTF();
		this.second=in.readUTF();
	}

	public int compareTo(StringPart o){
		if(!this.first.equals(o.getFirst())){
			return first.compareTo(o.getFirst());
		}else{
			if(!this.second.equals(o.getFirst())){
				return second.compareTo(o.getFirst());
			}else{
				return 0;
		}}
	}
	
	public int hasCode(){
		final int prime=31;
		int result=1;
		result=prime*result+((first==null)?0:first.hashCode());
		result=prime*result+((second==null)?0:second.hashCode());
		return result;
	}
	
	public boolean equels(Object obj){
		
		if(this==obj)
			return true;
		if(obj==null)
			return false;
		if(getClass()!=obj.getClass())
			return false;
		StringPart other=(StringPart)obj;
		if(first==null){
			if(other.first!=null)
				return false;
		}else if(!first.equals(other.first))
			return false;
		if(second==null){
			if(other.second!=null)
				return false;
		}else if(!second.equals(other.second))
			return false;
		
		return true;
	}
	
	
}