package in.dream_lab.bm.stream_iot.tasks.annotate;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

/**
 * @author shilpa
 *
 */
public class Annotate extends AbstractTask<String,String>
{
	private static Map<String, String> annotationMap;
	private static String filePath ; 
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int useMsgField;
	//private String sampleData = "024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,2013-01-14 04:23:00,200,10,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,0.00,0.50,13.00,4.80,70.30";
	
	@Override
	public void setup(Logger l_, Properties p_) 
	{
		super.setup(l_, p_);
		annotationMap = new HashMap<String, String>();
		try
		{
			synchronized (SETUP_LOCK)
			{
				if(!doneSetup) 
				{
					// Check usefield 
					useMsgField = Integer.parseInt(p_.getProperty("ANNOTATE.ANNOTATE_MSG_USE_FIELD", "0"));
					// read file path for anotations
					filePath = (p_.getProperty("ANNOTATE.ANNOTATE_FILE_PATH"));
				    // read file content 
					FileReader reader = new FileReader(filePath); 
					BufferedReader br = new BufferedReader(reader);
		    		String s ;
		    		String [] annotation ;
		    		
			    	// populate hashmap with anotations
		    		while((s = br.readLine()) != null)
					{
						annotation = s.split(":");
						assert annotation.length == 2;
						annotationMap.put(annotation[0], annotation[1]);
					}
		    		doneSetup = true;
				}
			}
			}
			catch (IOException e) 
			{
				e.printStackTrace();	
				l.warn("Exception in initializing AnnotationMap from file: "+filePath, e);
			}
	}

	@Override
	protected Float doTaskLogic(Map<String,String> map) 
	{
		// reading the meta and obsvalue from string
		String in ; 
		String annotateKey;
		in = (String) map.get(AbstractTask.DEFAULT_KEY);
		annotateKey = in.split(",")[useMsgField];
		
		// Fetch annotated values from hashmap corresponding to this key 
		String annotation = annotationMap.get(annotateKey);
		
		if(annotation != null)
		{
		    String annotatedValue = new StringBuffer(in).append(",").append(annotation).toString();
		    setLastResult(annotatedValue);
		}
		return 0f;
	}
	
	
}
