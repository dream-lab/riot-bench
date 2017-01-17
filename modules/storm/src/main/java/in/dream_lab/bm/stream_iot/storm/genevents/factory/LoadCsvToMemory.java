package in.dream_lab.bm.stream_iot.storm.genevents.factory;

import com.opencsv.CSVReader;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;


/* 
 * Splits the CSV file in round-robin manner and stores it to individual files
 * based on the number of threads
 */
public class LoadCsvToMemory {
	public static Logger LOG = LoggerFactory.getLogger(LoadCsvToMemory.class); 
		
	public static ArrayList<RowClass> loadListFromCSV(String csvFileName) throws IOException{
		CSVReader reader = new CSVReader(new FileReader(csvFileName));
	     String [] nextLine;
	     int ctr = 0;
	     String [] headers = reader.readNext();  //use .intern() later
	     ArrayList<RowClass> arr = new ArrayList<RowClass>();
	     
	     while ((nextLine = reader.readNext()) != null) {
	        // nextLine[] is an array of values from the line
	        //System.out.println(nextLine[0] +  "  "  + nextLine[1] + "   " + nextLine[2] + "  " + nextLine[3]  + "  etc...");
	    	
	        HashMap<String, String> map = new HashMap<String, String>();
	        for(int i=0; i<nextLine.length; i++){
	        	map.put(headers[i], nextLine[i]);
	        }
	        DateTime date = ISODateTimeFormat.dateTimeParser().parseDateTime(nextLine[0]);
	        long ts = date.getMillis();
	        RowClass rowClass = new RowClass(ts, map);
	        arr.add(rowClass);
	        
	        ctr++;
	     }
	     
	     return arr;
	}
	
	/**
	 * @param args
	 * @throws ParseException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ParseException, IOException {
		// TODO Auto-generated method stub
		String csvFileName = "/var/tmp/SyS/out/output" + 3 + ".csv";
		ArrayList<RowClass> arr = loadListFromCSV(csvFileName);
		System.out.println(arr.size());
		LOG.info("jkl");
	}
}