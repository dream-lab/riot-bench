package in.dream_lab.bm.stream_iot.tasks.driver;


import in.dream_lab.bm.stream_iot.tasks.statistics.Interpolation;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by anshushukla on 27/05/16.
 */
public class InterpolationTest extends Interpolation {


	private static Logger l; // TODO: Ensure logger is initialized before use

	/**
	 *
	 * @param l_
	 */
	public static void initLogger(Logger l_) {
		l = l_;
	}

	public static void main(String[] args) {


		Interpolation l1=new Interpolation();

		initLogger(LoggerFactory.getLogger("APP"));
		Properties p_=new Properties();
		try {
			p_.load(new FileReader("/home/shilpa/Sandbox/Repository/bmIOT/bm-iot/modules/tasks/src/main/resources/tasks.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		Float aFloat = null;
		l1.setup(l,p_ );
		for(int i = 0; i < 5 ; i++)
		{
			HashMap<String, String> map = new HashMap();
			map.put("temp", String.valueOf((i+1)));
			map.put("SENSORID", "abc");
			aFloat = l1.doTask(map);
			System.out.println("Returned value must be null" + aFloat);

		}

		HashMap<String, String> map = new HashMap();
		map.put("temp","null");
		map.put("SENSORID", "abc");
		
		aFloat = l1.doTask(map);
		System.out.println("Value returned now is " + aFloat);
		
		
		map = new HashMap();
		map.put("temp","6.0");
		map.put("SENSORID", "abc");
		aFloat = l1.doTask(map);
		System.out.println("Value returned should be null  " + aFloat);
		//            Float aFloat = l1.doTask((m.split(",")[5]));


		if(aFloat==null)
		{
			System.out.println("Null returned ");
			l.warn("Nothing");
		}
		else l.warn(String.valueOf(aFloat));
		System.out.println("Value returned is "+ aFloat);  
		l.warn(String.valueOf(l1.tearDown()));


	}

	//    @Test
	//    public void testBlockWindowAggTest1()
	//    {
	//
	//        BlockWindowCount l1=new BlockWindowCount();
	//
	//        initLogger(LoggerFactory.getLogger("APP"));
	//        Properties p_=new Properties();
	//
	//
	//        l1.setup(l,p_ );
	//        for(int c=0;c<25;c++) {
	//            String m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
	//            Float aFloat = l1.doTask(m);
	//            if(aFloat==null)  l.warn("Nothing");
	//            else l.warn(String.valueOf(aFloat));
	////            assertNotNull(aFloat);
	//        }
	//        l.warn(String.valueOf(l1.tearDown()));
	//    }

}
