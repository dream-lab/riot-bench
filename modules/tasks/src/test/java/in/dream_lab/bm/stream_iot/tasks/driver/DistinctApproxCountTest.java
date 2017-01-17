package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.DistinctApproxCount;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class DistinctApproxCountTest extends DistinctApproxCount {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        DistinctApproxCount l1=new DistinctApproxCount();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();

//        msgId,timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//        1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26
String  sensrID="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";

        l1.setup(l,p_ );

        for(int c=0;c<20;c++)
        {
        	HashMap<String, String> map = new HashMap();
            map.put(AbstractTask.DEFAULT_KEY,sensrID);
            
        	l.warn(String.valueOf(l1.doTask(map)));
        }
        l.warn(String.valueOf(l1.tearDown()));

    }


    @Test
    public void testDistinctApproxCountTest()
    {        DistinctApproxCount l1=new DistinctApproxCount();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();

//        msgId,timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//        1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26
        String  sensrID="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";

        l1.setup(l,p_ );
        Float distinctCount = null;
        for(int c=0;c<20;c++) 
        {
        	HashMap<String, String> map = new HashMap();
            map.put(AbstractTask.DEFAULT_KEY, sensrID);
            distinctCount = l1.doTask(map);
            l.warn(String.valueOf(distinctCount));
        }

        l.warn(String.valueOf(l1.tearDown()));

        Assert.assertNotNull(distinctCount);
//        assertNotNull(aFloat);
    }

}
