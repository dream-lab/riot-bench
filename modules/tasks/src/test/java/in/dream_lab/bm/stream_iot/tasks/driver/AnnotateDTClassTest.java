package in.dream_lab.bm.stream_iot.tasks.driver;


import in.dream_lab.bm.stream_iot.tasks.annotate.AnnotateDTClass;
import in.dream_lab.bm.stream_iot.tasks.statistics.Interpolation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class AnnotateDTClassTest extends Interpolation {


	private static Logger l; // TODO: Ensure logger is initialized before use

	/**
	 *
	 * @param l_
	 */
	public static void initLogger(Logger l_) {
		l = l_;
	}

	public static void main(String[] args) {


		AnnotateDTClass annotateDTClass=new AnnotateDTClass();

		initLogger(LoggerFactory.getLogger("APP"));
		Properties p_=new Properties();
		try {
			p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}

		Float aFloat = null;
		annotateDTClass.setup(l,p_ );


//		AzureTableRangeQueryTask.TaxiDropoffEntity result = new AzureTableRangeQueryTask.TaxiDropoffEntity();

//		Iterable<AzureTableRangeQueryTask.TaxiDropoffEntity> result = new Iterable<AzureTableRangeQueryTask.TaxiDropoffEntity> ();
//		result.setAirquality_raw("0.5");
//		result.setDropoff_datetime("1");
//		result.setDropoff_longitude("11");
//		result.setFare_amount("111");

//		Map<String,Iterable<AzureTableRangeQueryTask.TaxiDropoffEntity>> map=new HashMap<String,Iterable<AzureTableRangeQueryTask.TaxiDropoffEntity> > ();
//		map.put(AbstractTask.DEFAULT_KEY,result);


//		map.put(AbstractTask.DEFAULT_KEY,"0.5");

		
//		aFloat = annotateDTClass.doTask(map);
		System.out.println("Value returned now is " + aFloat);
		
		
		System.out.println("Value returned should be null  " + aFloat);



		if(aFloat==null)
		{
			System.out.println("Null returned ");
			l.warn("Nothing");
		}
		else l.warn(String.valueOf(aFloat));
		System.out.println("Value returned is "+ aFloat);  
		l.warn(String.valueOf(annotateDTClass.tearDown()));


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
