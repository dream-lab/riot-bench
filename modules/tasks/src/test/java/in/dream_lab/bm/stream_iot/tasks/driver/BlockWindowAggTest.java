package in.dream_lab.bm.stream_iot.tasks.driver;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.BlockWindowAverage;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class BlockWindowAggTest extends BlockWindowAverage {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        BlockWindowAverage l1=new BlockWindowAverage();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();


        l1.setup(l,p_ );
        for(int c=0;c<25;c++) {
            String m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
            HashMap<String, String> map = new HashMap();
            map.put(AbstractTask.DEFAULT_KEY, m);
                        Float aFloat = l1.doTask(map);
//            Float aFloat = l1.doTask((m.split(",")[5]));


            if(aFloat==null)  l.warn("Nothing");
            else l.warn(String.valueOf(aFloat));

        }
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
