package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionPredictor;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class LinearRegressionPredictorTest extends LinearRegressionPredictor{


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        LinearRegressionPredictor l1=new LinearRegressionPredictor();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();

        try {
//            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties"));
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_TAXI.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        l1.setup(l,p_ );
//        String input_row="-71.106167,42.372802,-0.1,65.3,0,367.38,26";  // SYS
        String input_row="42011,1.95,8.00"; // taxi
        
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AbstractTask.DEFAULT_KEY,input_row);
        
        l1.doTask(map);
        l1.tearDown();


    }

    @Test
    public void testLinearRegressionPredictorTest()
    {
        LinearRegressionPredictor l1=new LinearRegressionPredictor();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();


        l1.setup(l,p_ );
        String input_SYSrow="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AbstractTask.DEFAULT_KEY,input_SYSrow);
        
        Float res = l1.doTask(map);
        l1.tearDown();
        Assert.assertTrue(res>0);

//        assertEquals(0,new App().calculateSomething());
    }
}
