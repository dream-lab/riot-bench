package in.dream_lab.bm.stream_iot.tasks.driver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.statistics.KalmanFilter;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class KalmanFilterTest extends KalmanFilter {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        KalmanFilter l1 = new KalmanFilter();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_ = new Properties();


        l1.setup(l, p_);

        for (int c = 0; c < 20; c++)
        {
            HashMap<String, String> map = new HashMap<String, String>();
            map.put(AbstractTask.DEFAULT_KEY, "-71.106167,42.372802,-0.1,65.3,0,367.38,26");
        	l1.doTask(map);
        }
        l.warn(String.valueOf(l1.tearDown()));


    }

//    @Test
//    public void testKalmanFilterTest() {
//
//        System.out.println("plesse asse ekjvn dkn vdf  ****** ");
//
//
//        KalmanFilter l1 = new KalmanFilter();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_ = new Properties();
//
//
//        l1.setup(l, p_);
//
//        for (int c = 0; c < 20; c++)
//            l1.doTask("-71.106167,42.372802,-0.1,65.3,0,367.38,26");
//        l.warn(String.valueOf(l1.tearDown()));
//
////        Assert.assertEquals(0, new App().calculateSomething());
//    }

}
