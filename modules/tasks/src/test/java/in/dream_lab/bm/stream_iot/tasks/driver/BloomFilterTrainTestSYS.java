package in.dream_lab.bm.stream_iot.tasks.driver;

import in.dream_lab.bm.stream_iot.tasks.filter.BloomFilterTrain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * Created by anshushukla on 27/05/16.
 */
public class BloomFilterTrainTestSYS extends BloomFilterTrain {


    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {

        BloomFilterTrain bloomFilterTrain=new BloomFilterTrain();
        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        try {
            p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        bloomFilterTrain.setup(l,p_ );
//        msgId,timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//        1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26


//        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        bloomFilterTrain.doTask(m);
//        m="2,1443033000,ci4vjer3i000e02s7r2cj23gs,-43.1833012,-22.913566699999997,31.5,58.3,0,239.96,27";
//        bloomFilterTrain.doTask(m);


                Random rn=new Random();
        String tarindataFilePath="/Users/anshushukla/data/dataset-SYS-12min-100x.csv";
//        try (BufferedReader br = new BufferedReader(new FileReader(tarindataFilePath))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                System.out.println("line training :"+line);
//                String[] inputArray = line.split(",");
//
//                for(int c=4;c<inputArray.length;c++){
//                    int rndNumber=rn.nextInt(100);
//                    if(rndNumber<90) {// train at 90 % data
//                        System.out.println(inputArray[c] + "-" + rndNumber);
//                        bloomFilterTrain.doTask(inputArray[c]);
//                    }
//                    else
//                        System.out.println(rndNumber);
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        l.warn(String.valueOf(bloomFilterTrain.tearDown()));

//        BloomFilterTrain bloomFilterTrain=new BloomFilterTrain();
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//        try {
//            p_.load(new FileReader("src/main/resources/tasks.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        bloomFilterTrain.setup(l,p_ );
//
////        msgId,timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
////        1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26
//
//
////        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
////        bloomFilterTrain.doTask(m);
////        m="2,1443033000,ci4vjer3i000e02s7r2cj23gs,-43.1833012,-22.913566699999997,31.5,58.3,0,239.96,27";
////        bloomFilterTrain.doTask(m);
//
////        Actual SYS data sample
////        timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
////        2015-01-27T00:00:00.000Z,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26
//
//        Random rn=new Random();
//        String tarindataFilePath="/Users/anshushukla/data/dataset-SYS-12min-100x.csv";
//        try (BufferedReader br = new BufferedReader(new FileReader(tarindataFilePath))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                System.out.println("line training :"+line);
//                String[] inputArray = line.split(",");
//
//                for(int c=4;c<inputArray.length;c++){
//                    int rndNumber=rn.nextInt(100);
//                    if(rndNumber<90) {// train at 90 % data
//                        System.out.println(inputArray[c] + "-" + rndNumber);
//                        bloomFilterTrain.doTask(inputArray[c]);
//                    }
//                    else
//                        System.out.println(rndNumber);
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("Training done ");
//        l.warn(String.valueOf(bloomFilterTrain.tearDown()));
    }

//    @Test
//    public void testBloomFilterTrainTest()
//    {
//
//        BloomFilterTrain bloomFilterTrain=new BloomFilterTrain();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//
//
//        bloomFilterTrain.setup(l,p_ );
////        msgId,timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
////        1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26
//
//
//        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        bloomFilterTrain.doTask(m);
//        m="2,1443033000,ci4vjer3i000e02s7r2cj23gs,-43.1833012,-22.913566699999997,31.5,58.3,0,239.96,27";
//        bloomFilterTrain.doTask(m);
//
//        l.warn(String.valueOf(bloomFilterTrain.tearDown()));
////        assertEquals(0,new App().calculateSomething());
//    }
}
