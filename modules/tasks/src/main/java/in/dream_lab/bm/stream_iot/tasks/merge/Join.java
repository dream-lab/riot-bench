package in.dream_lab.bm.stream_iot.tasks.merge;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;

public class Join extends AbstractTask 
{
	
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int timestampField;
	
	private static HashMap<Integer, String> schemaMap = new HashMap<Integer, String>();
	
	public void setup(Logger l_, Properties p_)
	{
		
	}

	@Override
	protected Float doTaskLogic(Map map)
	{
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		try {
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		return null;
	} 
	
}

