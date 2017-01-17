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
public class BloomFilterTrainTestTAXI extends BloomFilterTrain {


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



        //    Actual SYS sample data
//      0         1         2       3           4          5      6   7       8
//    timestamp,source,longitude,latitude,temperature,humidity,light,dust,airquality_raw
//    2015-01-27T00:00:00.000Z,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26


//Actual TAXI sample data
//    0                 1           2               3           4                   5           6                   7               8                   9           10                  11
//taxi_identifier,hack_license,pickup_datetime,timestamp,trip_time_in_secs,    trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type    ,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
//024BE2DFD1B98AF1EA941DEDA63A15CB,9F5FE566E3EE57B85B723B71E370154C,2013-01-14 03:57:00,2013-01-14 04:23:00,1560,19.36,-73.953178,40.776016,-73.779190,40.645145,CRD,52.00,0.00,0.50,13.00,4.80,70.30


//        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        bloomFilterTrain.doTask(m);
//        m="2,1443033000,ci4vjer3i000e02s7r2cj23gs,-43.1833012,-22.913566699999997,31.5,58.3,0,239.96,27";
//        bloomFilterTrain.doTask(m);


                Random rn=new Random();
        String tarindataFilePath="/Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/test-TAXI-12min-1000x.csv";
//        try (BufferedReader br = new BufferedReader(new FileReader(tarindataFilePath))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                System.out.println("line training :"+line);
//                String[] inputArray = line.split(",");
//                for(int c=11;c<inputArray.length;c++){
//                    int rndNumber=rn.nextInt(100);
//                    if(rndNumber<90) {// train at 90 % data
//                        System.out.println(inputArray[c] + "-" + rndNumber);
//                        bloomFilterTrain.doTask(inputArray[4]);
//                        bloomFilterTrain.doTask(inputArray[5]);
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
