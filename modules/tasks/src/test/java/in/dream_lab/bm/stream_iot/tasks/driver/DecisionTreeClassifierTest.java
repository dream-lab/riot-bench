package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeClassify;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by anshushukla on 27/05/16.
 */
public class DecisionTreeClassifierTest extends DecisionTreeClassify {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        DecisionTreeClassify l1=new DecisionTreeClassify();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        try {
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties"));
//            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_TAXI.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        l1.setup(l,p_ );
//        for(int c=0;c<20;c++)
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, "-81.10,42.37,10.1,65.3,0"); // CITY DATASET
//        map.put(AbstractTask.DEFAULT_KEY, "4205,1.95,8.00");  // TAXI DATASET
        
        l1.doTask(map);
        l.warn(String.valueOf(l1.tearDown()));


    }

    @Test
    public void testDecisionTreeClassifierTest()
    {
        DecisionTreeClassify l1=new DecisionTreeClassify();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();


        l1.setup(l,p_ );
//        for(int c=0;c<20;c++)
        
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, "-71.106167,42.372802,100000.1,65.3,0,367.38,26");
        
        Float enumIndex = l1.doTask(map);
        l.warn(String.valueOf(l1.tearDown()));

        assertTrue(enumIndex>0); // index fo result in enum can never be negative
//        assertEquals(0,new App().calculateSomething());
    }
}
