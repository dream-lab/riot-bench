//package in.dream_lab.bm.stream_iot.tasks.driver;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import in.dream_lab.bm.stream_iot.tasks.statistics.SecondOrderMoment;
//
//import java.util.Properties;
//
///**
// * Created by anshushukla on 27/05/16.
// */
//public class SecondOrderMomentTest extends SecondOrderMoment {
//
//
//    private static Logger l; // TODO: Ensure logger is initialized before use
//
//    /**
//     *
//     * @param l_
//     */
//    public static void initLogger(Logger l_) {
//        l = l_;
//    }
//
//    public static void main(String[] args) {
//
//
//        SecondOrderMoment l1=new SecondOrderMoment();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//
//
//        l1.setup(l,p_ );
//
//        String m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//
//        for(int c=0;c<20;c++) {
////                l1.doTask(String.valueOf(m));    // uncomment to test result for skewed data
//            l1.doTask((-71.106167 + c) + ",42.372802,-0.1,65.3,0,367.38,26"); // for uniformly distributed data
//            l1.doTask((-71.106167 + c) + ",42.372802,-0.1,65.3,0,367.38,26"); // for uniformly distributed data
//        }
//
//        l.warn(String.valueOf(l1.tearDown()));
//
//    }
//
////    @Test
////    public void testSecondOrderMomentTest()
////    {
////        SecondOrderMoment l1=new SecondOrderMoment();
////
////        initLogger(LoggerFactory.getLogger("APP"));
////        Properties p_=new Properties();
////
////
////        l1.setup(l,p_ );
////
////        String m="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
////
////        for(int c=0;c<20;c++) {
//////                l1.doTask(String.valueOf(m));    // uncomment to test result for skewed data
////            l1.doTask((-71.106167 + c) + ",42.372802,-0.1,65.3,0,367.38,26"); // for uniformly distributed data
//////            l1.doTask((-71.106167 + c) + ",42.372802,-0.1,65.3,0,367.38,26"); // for uniformly distributed data
////        }
////
//////        l.warn(String.valueOf(l1.tearDown()));
////        assertEquals(0,new App().calculateSomething());
////    }
//}
