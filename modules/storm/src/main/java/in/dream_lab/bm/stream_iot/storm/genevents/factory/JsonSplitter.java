package in.dream_lab.bm.stream_iot.storm.genevents.factory;

import com.opencsv.CSVReader;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;


/* 
 * Splits the JSON file in round-robin manner and stores it to individual files
 * based on the number of threads
 */
public class JsonSplitter {
	public static Logger LOG = LoggerFactory.getLogger(CsvSplitter.class);
	
	public static int numThreads;
	public static int peakRate;

	public static List<String> extractHeadersFromCSV(String inputFileName){
		try {
			CSVReader reader = new CSVReader(new FileReader(inputFileName));
			String[] headers = reader.readNext(); // use .intern() later
			reader.close();
			List<String> headerList = new ArrayList<String>();
			for(String s : headers){
				headerList.add(s);
			}
			return headerList;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	//Assumes sorted on timestamp csv file
	//It also treats the first event to be at 0 relative time
	public static List<TableClass> roundRobinSplitJsonToMemory(String inputSortedCSVFileName, int numThreads, double accFactor, String datasetType) throws IOException{
		MyCSVReader reader = new MyCSVReader(inputSortedCSVFileName);
		String[] nextLine;
		int ctr = 0;
		String[] headers = reader.readLine(); // use .intern() later
		List<String> headerList = new ArrayList<String>();
		for(String s: headers){
			headerList.add(s);
		}
		
		List<TableClass> tableList = new ArrayList<TableClass>();
		for (int i = 0; i < numThreads; i++) {
			TableClass tableClass = new TableClass();
			tableClass.setHeader(headerList);
			tableList.add(tableClass);
		}
		
		TableClass tableClass = null;
		boolean flag = true;
		long startTs = 0, deltaTs = 0;
		
		//CODE TO ACCOMODATE PLUG DATASET SPECIAL CASE TO RUN IT FOR 10 MINS
		int numMins = 90000;  // Keeping it fixed for current set of experiments
		Double cutOffTimeStamp=0.0;//int msgs=0;
		MyJSONReader jsonReader = new MyJSONReader(inputSortedCSVFileName);
		nextLine = jsonReader.readLine();
		while ((nextLine = jsonReader.readLine()) != null) 
		{
			List<String> row = new ArrayList<String>();
			for (int i = 0; i < nextLine.length; i++) {
				row.add(nextLine[i]);
			}
			
			tableClass = tableList.get(ctr);
			ctr = (ctr + 1) % numThreads;

			int timestampColIndex = 0;
			DateTime date = null;
			datasetType = "SENML";
			if(datasetType.equals("TAXI")){
				timestampColIndex = 3;
				date = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(nextLine[timestampColIndex]);
			}
			else if(datasetType.equals("SYS")){
				
				timestampColIndex = 0;
				date = ISODateTimeFormat.dateTimeParser().parseDateTime(
						nextLine[timestampColIndex]);
				System.out.println("date "+ date);
			}
			else if(datasetType.equals("PLUG")){
				timestampColIndex = 1;
				date = new DateTime(Long.parseLong(nextLine[timestampColIndex])*1000);
			}
			else if(datasetType.equals("SENML")){
				timestampColIndex = 0;
				date = new DateTime(Long.parseLong(nextLine[timestampColIndex]));
			}

			long ts = date.getMillis();
			if(flag){
				startTs = ts; 
				flag = false;
				cutOffTimeStamp = startTs + numMins * (1.0/accFactor) * 60 * 1000;  // accFactor is actually the scaling factor or deceleration factor
			}
			
			if(ts > cutOffTimeStamp)
			{
				break; // No More data to be loaded
			}
			
			deltaTs = ts - startTs;
			deltaTs = (long) (accFactor * deltaTs);
			tableClass.append(deltaTs, row);
			//System.out.println("ts " + (ts - startTs) + " deltaTs " + deltaTs);
		}

		reader.close();
		return tableList;
	}
	
	public static void roundRobinSplitCsvToFiles(String inputFileName, int numThreads) throws IOException{
		
		BufferedReader bReader = new BufferedReader(new FileReader(inputFileName));
	    String headerLine = bReader.readLine();
	    System.out.println(headerLine);
	    String line;
	    
	    BufferedWriter [] bWriters = new BufferedWriter[numThreads];
	    
	    for(int i=0; i<numThreads; i++){
	    	bWriters[i] = new BufferedWriter(new FileWriter("/var/tmp/SyS/out/output" + i + ".csv"));
	    	bWriters[i].write(headerLine);
	    	bWriters[i].newLine();
	    }
	    
	    int ctr = 0;
	    while((line = bReader.readLine()) != null){
	    	bWriters[ctr].write(line);
	    	bWriters[ctr].newLine();
	    	ctr = (ctr+1)%numThreads;
	    }
	    
	    bReader.close();
	    for(int i=0; i<numThreads; i++){
	    	bWriters[i].flush();
	    	bWriters[i].close();
	    }
	}
	
	/**
	 * @param args
	 * @throws ParseException
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ParseException, IOException {
		// TODO Auto-generated method stub
		int defaultNumThreads = 4, defaultPeakRate = 100;
		switch(args.length){
			case 2: numThreads = Integer.parseInt(args[0]);
					peakRate = Integer.parseInt(args[1]); break; 
			case 1: numThreads = Integer.parseInt(args[0]);
					peakRate = defaultPeakRate; break;
			case 0: numThreads = defaultNumThreads;
					peakRate = defaultPeakRate; break;
			default: LOG.warn("Invalid Number of Arguments! args = numThreads peakRate"); return ;
		}
		
//		String inputFileName = "/var/tmp/SyS/bangalore.csv";
		String inputFileName = "/Users/anshushukla/data/trytry.csv";

		//roundRobinSplitCsvToFiles(inputFileName, numThreads);
		List<TableClass> list = roundRobinSplitJsonToMemory(inputFileName, numThreads, 0.001,"SYS");
		for(int i=0; i<numThreads; i++)
		System.out.println(list.get(i).getRows().size());
		
		//Test Iterator Functionality
//		for(RowClass t : list.get(0)){
//			System.out.println(t);
//		}
				
		LOG.info("jkl");
	}
}

interface MyReader {
	String[] readLine() throws IOException;
	void init() throws FileNotFoundException;
	public void close() throws IOException;
}
 class MyCSVReader implements MyReader
{
    public CSVReader reader;
	public String inputFileName;
	public MyCSVReader(String inputFileName_){
		inputFileName = inputFileName_;
		try {
			init();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
	
	public void init() throws FileNotFoundException{
		reader = new CSVReader(new FileReader(inputFileName));
	}
	
	public void close() throws IOException{
		reader.close();
	}
	
	@Override
	public String[] readLine() throws IOException {
		return reader.readNext();
	}	
}
 
 class MyJSONReader implements MyReader
 {
    public BufferedReader bufferedReader;
    public FileReader reader;
 	public String inputFileName;
 	public MyJSONReader(String inputFileName_){
 		inputFileName = inputFileName_;
 		try {
 			init();
 		} catch (FileNotFoundException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		} catch (IOException e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		}			
 	}
 	
 	public void init() throws FileNotFoundException
 	{
 		reader = new FileReader(inputFileName);
 		bufferedReader = new BufferedReader(reader);
 	}
 	
 	public void close() throws IOException{
 		bufferedReader.close();
 	}
 	
 	@Override
 	public String[] readLine() throws IOException
 	{
 		String nextLine = bufferedReader.readLine();
 		while (nextLine != null) 
 		{
			String [] values = new String[2] ;
	 		values[0] = (nextLine.split(","))[0];
	 		values[1] = nextLine.substring(nextLine.indexOf(",") +1 );
	 		return values;
 		}
 		return null;
 	}	
 }

