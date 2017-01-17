package in.dream_lab.bm.stream_iot.storm.genevents.utils;

import org.apache.storm.task.TopologyContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class GlobalConstants {
	public static final int numThreads = 1;  // change it to 4 etc
	public static final double accFactor = 0.01;
	public static final int thresholdFlushToLog = 0;  //100
	public static final String defaultBoltDirectory = "/var/tmp/";  //can be changed to tetc-final/dataset/
	public static String dataSetType = "PLUG";  //IT CAN BE STALE USE WITH CAUTION
	public static String expNum = "0";  //IT CAN BE STALE USE WITH CAUTION

	public static boolean isCharInRange(char ch, char min, char max){
		if(ch >= min && ch <= max){
			return true;
		}
		else{
			return false;
		}
	}
	
	public static void setDataSetType(String experiRunID){
		if(experiRunID.indexOf("TAXI") != -1){
			GlobalConstants.dataSetType = "TAXI"; 
		}
		else if(experiRunID.indexOf("SYS") != -1){
			GlobalConstants.dataSetType = "SYS";
		}
		else if(experiRunID.indexOf("PLUG") != -1){
			GlobalConstants.dataSetType = "PLUG";
		}
	}
	
	public static String getDataSetTypeFromRunID(String experiRunID){
		if(experiRunID.indexOf("TAXI") != -1){
			return "TAXI"; 
		}
		else if(experiRunID.indexOf("SYS") != -1){
			return "SYS";
		}
		else if(experiRunID.indexOf("PLUG") != -1){
			return "PLUG";
		}
		return null;
	}



	public static void setExperimentNumber(String experiRunID)
	{
		 GlobalConstants.expNum=experiRunID;
	}

	public static String getExperimentNumber()
	{
		return  GlobalConstants.expNum;
	}
	
	public static void createBoltIdentifyingFiles(TopologyContext topologyContext){
		String componentName = topologyContext.getThisComponentId();
	     Long ts = System.currentTimeMillis();
	     String fileName = "bolt-" + ts + "-" + componentName + ".log";
	     File file = new File(GlobalConstants.defaultBoltDirectory + fileName);
	     try {
			FileWriter fw = new FileWriter(file);
			 BufferedWriter bw = new BufferedWriter(fw);
			 String rowString = InetAddress.getLocalHost().getHostName() + "," + Thread.currentThread().getName() + "," + componentName + "," + ts;
			 bw.write(rowString);
			 bw.flush();
			 bw.close();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
