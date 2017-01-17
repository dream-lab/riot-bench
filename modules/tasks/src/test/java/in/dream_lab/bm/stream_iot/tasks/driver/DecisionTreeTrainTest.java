package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeTrain;

import java.util.HashMap;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class DecisionTreeTrainTest extends DecisionTreeTrain {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        DecisionTreeTrain decisionTreeTrain=new DecisionTreeTrain();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();


        decisionTreeTrain.setup(l,p_ );

//        String dummy="-71.106167,42.372802,-0.1,65.3,0,367.38,26,Good";
        HashMap<String, String> map;
        for(int c=0;c<1150;c++)
        {	
        	map = new HashMap();
            map.put(AbstractTask.DEFAULT_KEY, "-71.106167,42.372802,-0.1,65.3,0,"+(367.38+c*20)+",26,Good");
        	decisionTreeTrain.doTask(map);
        }
        l.warn(String.valueOf(decisionTreeTrain.tearDown()));


    }

    @Test
    public void testDecisionTreeTrainTest()
    {
        DecisionTreeTrain decisionTreeTrain=new DecisionTreeTrain();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();

        decisionTreeTrain.setup(l,p_ );

//        String dummy="-71.106167,42.372802,-0.1,65.3,0,367.38,26,Good";

        for(int c=0;c<1150;c++) 
        {
        	HashMap<String, String> map = new HashMap();
            map.put(AbstractTask.DEFAULT_KEY, "-71.106167,42.372802,-0.1,65.3,0,"+(367.38+c*20)+",26,Good");
           
        	Float trainSuccess =decisionTreeTrain.doTask(map);
            Assert.assertTrue(trainSuccess==0); // train Success flag as return
            
            map = new HashMap();
            map.put(AbstractTask.DEFAULT_KEY, "-71.106167,42.372802,-0.1,65.3,0,"+(367.38+c)+",2600,Bad");
            
            trainSuccess =decisionTreeTrain.doTask(map);
            Assert.assertTrue(trainSuccess==0); // train Success flag as return
        }

        l.warn(String.valueOf(decisionTreeTrain.tearDown()));


    }

}
