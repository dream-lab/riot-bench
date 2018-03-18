package in.dream_lab.bm.stream_iot.tasks.parse;

import org.joda.time.format.DateTimeFormat;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

/**
 * @author simmhan, shilpa
 *
 */
public class SenMLParse extends AbstractTask<String,Map> 
{
	
	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	private ArrayList <String> senMLlist;
	private static int useMsgField;
	private String sampledata;
	
	public void setup(Logger l_, Properties p_) 
	{
		super.setup(l_, p_);
		synchronized (SETUP_LOCK)
		{
			if(!doneSetup) 
			{
				useMsgField = Integer.parseInt(p_.getProperty("PARSE.SENML.USE_MSG_FIELD", "0"));
				doneSetup=true;
			}
			sampledata = p_.getProperty("PARSE.SENML.SAMPLEDATA");
		}
		
	}
	
	@Override
	protected Float doTaskLogic(Map map) 
	{
		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject ; 
	
		try 
		{
			String m;
			if(useMsgField == -1 )
				m = sampledata;
			else
				 m = (String)map.get(AbstractTask.DEFAULT_KEY);
			jsonObject = (JSONObject) jsonParser.parse(m);
			/*this is for TAXI dataset*/
			long baseTime =   (long) (jsonObject.get("bt") == null  ? 0L : jsonObject.get("bt")) ; // for sys and taxi
//			long baseTime =   Long.parseLong(((String)jsonObject.get("bt"))) ;     // for fit dataset
			String baseUnit = (String)(( jsonObject.get("bu") == null ) ? null : jsonObject.get("bu") );
			String baseName = (String)(( jsonObject.get("bn") == null ) ? null : jsonObject.get("bn") );
			JSONArray jsonArr = (JSONArray) jsonObject.get("e");
			Object v;
			String n, u;
			long t;
			HashMap mapkeyValues = new HashMap<String, String>();
			mapkeyValues.put("timestamp", String.valueOf(baseTime));
			for(int j=0; j<jsonArr.size(); j++)
			{
				jsonObject = (JSONObject) jsonArr.get(j);
				
				v = (jsonObject.get("v") == null ) ? (String) jsonObject.get("sv") : (String) jsonObject.get("v");				

				t = (jsonObject.get("t") == null) ? 0: (long)jsonObject.get("t");
				
				t = t+ baseTime;
				
				/* if name does not exist, consider base name */
				n = (jsonObject.get("n") == null) ? baseName :(String) jsonObject.get("n");					

				u = (jsonObject.get("u") == null) ? baseUnit :(String) jsonObject.get("u");

				/* Add to  Hashmap  each key value pair */
				mapkeyValues.put(n, v);			
			}
			super.setLastResult(mapkeyValues);	
			return null; 
		}
		catch (Exception e)
		 {
			e.printStackTrace();
		 }
		return 0.0f;
	}	
}
