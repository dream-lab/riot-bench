package in.dream_lab.bm.stream_iot.tasks;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class Utils 
{
	HashMap<Integer, String> schemaMap = new HashMap<Integer, String>();
	int timestampField = 0 ;
	public Utils(String schemaFilePath) 
	{
		try 
		{
			FileReader reader = new FileReader(schemaFilePath);
			BufferedReader br = new BufferedReader(reader);
			String s, schema;
			String column, unit, type;
			String [] columns ,  units, types;
			
			/* read a csv schema file */
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
			System.out.println("In Utils. Size of hash map "+ schemaMap.size());
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	/* csv to senml convert function
	 * Shilpa
	 */
	public JSONObject csvToSenMl(String s)
	{		

		JSONArray jsonArr = new JSONArray();
		JSONObject obj = new JSONObject();
		try
		{
			String [] val = s.split(",");
			JSONObject finalSenML = new JSONObject();
			/*FIXME This part needs to be adjusted depending on timestamp date format of data */
			Date d = (Date) (new SimpleDateFormat("yyyy-mm-dd hh:mm:ss")).parse(val[timestampField]);
			finalSenML.put("bt", d.getTime());
			String[] sch;
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
			return finalSenML;
		}
		catch(Exception e )
		{
			e.printStackTrace();
		}
		return null;
	}
	
	/*For testing 
	 * added main method 
	*/
	public static void main(String[] args) 
	{
	  Utils util = new Utils("/home/shilpa/Sandbox/java/JsonExperiments/src/main/resources/taxi-schema.txt" );
	  /* read the data file */
	  try 
		{
			FileReader reader = new FileReader("/home/shilpa/Sandbox/java/JsonExperiments/src/main/resources/taxi-data.txt");
			BufferedReader br = new BufferedReader(reader);
			String s = br.readLine();
			while(s != null)
			{
				util.csvToSenMl(s);
				s = br.readLine();
			}
					
		}
	  catch(Exception e)
	  {
		  
	  }
	}
}
