package in.dream_lab.bm.stream_iot.tasks.driver;

import in.dream_lab.bm.stream_iot.tasks.filter.BloomFilterCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by anshushukla on 27/05/16.
 */
public class BloomFilterCheckTestTAXI extends BloomFilterCheck {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {


        BloomFilterCheck l1=new BloomFilterCheck();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        try {
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        l1.setup(l,p_ );
//        String m="-71.106167";
//        System.out.println(String.valueOf(l1.doTask(m)));


        String tarindataFilePath="/Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/test-TAXI-12min-1000x.csv";
//        try (BufferedReader br = new BufferedReader(new FileReader(tarindataFilePath))) {
//            String line;
//            while ((line = br.readLine()) != null) {
////                System.out.println("line training :"+line);
//                String[] inputArray = line.split(",");
//
//                for(int c=11;c<inputArray.length;c++){
//                    Float output=l1.doTask(inputArray[c]);
//                    if(output==0)
//                        System.out.println("Ans:"+output);
//
//                     output=l1.doTask(inputArray[4]);
//                    if(output==0)
//                        System.out.println("Ans:"+output);
//                     output=l1.doTask(inputArray[5]);
//                    if(output==0)
//                        System.out.println("Ans:"+output);
//
////                    if(rndNumber<90) {// train at 90 % data
////                        System.out.println(inputArray[c] + "-" + rndNumber);
////                        bloomFilterTrain.doTask(inputArray[c]);
////                    }
//
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }




//        // to check false
//        String m1="11,1443033000,ci114ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        l1.doTask(m1);
//        l.warn(String.valueOf(l1.tearDown()));

    }

}
