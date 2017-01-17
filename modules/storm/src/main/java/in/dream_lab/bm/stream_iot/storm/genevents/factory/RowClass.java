package in.dream_lab.bm.stream_iot.storm.genevents.factory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RowClass {
	long ts;  //DateTime
	Map<String, String> payLoad;
	
	public RowClass(long ts, Map<String, String> payLoad){
		this.ts = ts;
		this.payLoad = payLoad;
	}
	
	public RowClass(long ts, List<String> header, List<String> row){
		this.ts = ts;
		this.payLoad = new HashMap<String, String>();
		for(int i=0; i<header.size(); i++){
			this.payLoad.put(header.get(i), row.get(i));
		}
	}
	
	public RowClass(){
		ts = 0;
		payLoad = null;
	}
	
	public long getTs() {
		return ts;
	}
	public void setTs(long ts) {
		this.ts = ts;
	}
	public Map<String, String> getPayLoad() {
		return payLoad;
	}
	public void setPayLoad(Map<String, String> payLoad) {
		this.payLoad = payLoad;
	}
	
	public String toString(){
		String out = ts + "," + payLoad;
		return out;	
	}
}
