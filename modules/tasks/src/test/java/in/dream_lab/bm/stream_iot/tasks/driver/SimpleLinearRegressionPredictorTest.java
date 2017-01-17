//package in.dream_lab.bm.stream_iot.tasks.driver;
//
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import in.dream_lab.bm.stream_iot.tasks.predict.SimpleLinearRegressionPredictor;
//
//import java.util.Properties;
//
//import static org.junit.Assert.assertTrue;
//
///**
// * Created by anshushukla on 27/05/16.
// */
//public class SimpleLinearRegressionPredictorTest extends SimpleLinearRegressionPredictor{
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
//        SimpleLinearRegressionPredictor l1=new SimpleLinearRegressionPredictor();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//
//        //Logic for SYS
//        //timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//        String m="2015-01-15T00:00:00.000Z,ci4q0adco000002t9qu491siy,-23.002739,-43.337678,34.1,45.3,0,1819.2,44";
//
//        l1.setup(l,p_ );
////        String input_SYSrow="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//
//        for(int c=0;c<20;c++)
//            l1.doTask("2015-01-15T00:00:00.000Z,ci4q0adco000002t9qu491siy,-23.002739*2,"+(-43.337678+5*c)+",34.1,45.3,-1,1819.2,c*100*44");
//
//        l1.tearDown();
//
//
//    }
//    @Test
//    public void testSimpleLinearRegressionPredictorTest()
//    {
//        SimpleLinearRegressionPredictor l1=new SimpleLinearRegressionPredictor();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//
//        //Logic for SYS
//        //timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//        String m="2015-01-15T00:00:00.000Z,ci4q0adco000002t9qu491siy,-23.002739,-43.337678,34.1,45.3,0,1819.2,44";
//
//        l1.setup(l,p_ );
////        String input_SYSrow="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//
//        for(int c=0;c<20;c++) {
//            Float predLength = l1.doTask("2015-01-15T00:00:00.000Z,ci4q0adco000002t9qu491siy,-23.002739," + (-43.337678 + 5 * c) + ",34.1,45.3,-1,1819.2,100*44");
//            System.out.println("predLength -"+predLength);
//
//            assertTrue(predLength>0); // Need to be updated
//        }
//
//        l1.tearDown();
//
//        
////        assertEquals(0,new App().calculateSomething());
//    }
//}
