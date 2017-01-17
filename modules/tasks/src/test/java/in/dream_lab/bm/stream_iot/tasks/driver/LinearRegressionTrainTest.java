package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.LinearRegressionTrain;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class LinearRegressionTrainTest extends LinearRegressionTrain {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        LinearRegressionTrain l1=new LinearRegressionTrain();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();


        l1.setup(l,p_ );

//        String input_SYSrow="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        l1.doTask(input_SYSrow);
        HashMap<String, String> map ;
        for(int c=0;c<5000;c++) 
        {// Dummy data to see changes in model
        	map = new HashMap<String, String>();
            map.put(AbstractTask.DEFAULT_KEY,"-71.106167," + c * 42.372802 + ",-0.1,65.3,0,367.38,26,Good");
        	
            l1.doTask(map);
            
            map = new HashMap<String, String>();
            map.put(AbstractTask.DEFAULT_KEY,"-711111.106167,"+(c*4.372802)+",1110.1,11165.3,0,"+(367.38+c)+",26000,BAD");
            
            l1.doTask(map);
        }

        l1.tearDown();


    }

    @Test
    public void testLinearRegressionTrainTest()
    {
//        LinearRegressionTrain l1=new LinearRegressionTrain();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//
//
//        l1.setup(l,p_ );
//
////        String input_SYSrow="-71.106167,42.372802,-0.1,65.3,0,367.38,26";
////        l1.doTask(input_SYSrow);
//
//        for(int c=0;c<5000;c++) {// Dummy data to see changes in model
//            Float trainSuccess= l1.doTask("-71.106167," + c * 42.372802 + ",-0.1,65.3,0,367.38,26,Good");
//            Assert.assertTrue(trainSuccess==0); // train Success flag as return
//            trainSuccess=l1.doTask("-711111.106167,"+(c*4.372802)+",1110.1,11165.3,0,"+(367.38+c)+",26000,BAD");
//            Assert.assertTrue(trainSuccess==0); // train Success flag as return
//
//        }
//
//        l1.tearDown();

    }
}
