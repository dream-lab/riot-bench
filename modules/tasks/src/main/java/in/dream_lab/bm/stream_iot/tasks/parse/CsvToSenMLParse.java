package in.dream_lab.bm.stream_iot.tasks.parse;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.acl.LastOwnerException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;

/**
 * @author shilpa
 *
 */
public class CsvToSenMLParse extends AbstractTask<String,String> 
{
	
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int timestampField;
	/* for task benchmarking */
	private String sampledata = "";
	private static int useMsgField;
	/* hashmap of schema in the form fieldnum as key and  
	 * coulumn name,unit and type of data separated by comma as value*/
	private static HashMap<Integer, String> schemaMap = new HashMap<Integer, String>();
	
	public void setup(Logger l_, Properties p_)
	{
		FileReader reader;
		BufferedReader br;
		String column, unit, type;
		String [] columns, units, types;
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) 
		{
		if(!doneSetup) 
		{ 
			String schemaFilePath = p_.getProperty("PARSE.CSV_SCHEMA_WITH_ANNOTATEDFIELDS_FILEPATH");
			useMsgField =Integer.parseInt(p_.getProperty("PARSE.CSV_SENML_USE_MSG_FIELD"));
			try 
			{
				reader = new FileReader(schemaFilePath);
				br = new BufferedReader(reader);
				column = br.readLine();
				unit = br.readLine();
				type = br.readLine();
				columns = column.split(",");
				units = unit.split(",");
				types = type.split(",");
				br.close();
				reader.close();
				long bt;
				for(int i = 0; i< columns.length ;i++)
				{
					if(columns[i].equals("timestamp"))	
						timestampField = i;
					schemaMap.put(i,columns[i]+","+units[i]+","+types[i]);
				}
			
			} catch (Exception e) 
			{
				e.printStackTrace();
				l_.warn("Exception in reading senML file: " + schemaFilePath, e);
			}
			doneSetup=true;
		}
		sampledata = "024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,2013-01-14 04:23:00,200,10,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,0.00,0.50,13.00,4.80,70.30,uber,sam,Houston";
	}
}

	@Override
	protected Float doTaskLogic(Map map) 
	{
		JSONObject obj;
		JSONArray jsonArr = new JSONArray();
		JSONObject finalSenML;
		try
		{
			String m ;
			if(useMsgField == -1)
				m= sampledata;
			else
				m = (String)map.get(AbstractTask.DEFAULT_KEY);
			String [] val = m.split(",");
			String[] sch;
			jsonArr = new JSONArray();
			val = m.split(",");
			
			finalSenML = new JSONObject();
			finalSenML.put("bt",val[timestampField] );
			for(int i = 0; i< schemaMap.size(); i++)
			{  	
				sch = ((String)schemaMap.get(i)).split(",");
				if(i != timestampField)
				{
					obj = new JSONObject();
					obj.put("n", sch[0]);
					obj.put(sch[2], val[i]);
					obj.put("u", sch[1]);
					jsonArr.add(obj);
				}
			}
			finalSenML.put("e", jsonArr);
			setLastResult(finalSenML.toJSONString());
			return null;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	} 
	
}
