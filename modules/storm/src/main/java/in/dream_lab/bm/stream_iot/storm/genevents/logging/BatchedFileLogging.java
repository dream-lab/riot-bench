///*package in.dream_lab.genevents.logging;
//
//import in.dream_lab.genevents.utils.GlobalConstants;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.net.InetAddress;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.Map;
//
//
// //* Created by anshushukla on 20/05/15.
//
//
//public class BatchedFileLogging  {
//    int counter=0;
//    Map<Long,String> batch=new HashMap<Long,String>();
//    FileWriter fstream;
//    BufferedWriter out;
//    String csvFileNameOut;
//    int threshold; //Count of rows after which the map should be flushed to log file
//
//    String logStringPrefix;
//
//    public BatchedFileLogging(){
//        this.csvFileNameOut = "/Users/anshushukla/data/values.csv";
//    }
//
//    public BatchedFileLogging(String csvFileNameOut, String componentName){
//        this.csvFileNameOut = csvFileNameOut;
//        try {
//            this.fstream = new FileWriter(this.csvFileNameOut,false);
//            this.out = new BufferedWriter(fstream);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        this.threshold = GlobalConstants.thresholdFlushToLog; //2000 etc
//        try {
//            this.logStringPrefix = InetAddress.getLocalHost().getHostName() + "," + Thread.currentThread().getName() + "," + componentName;
//            //System.out.println(this.logStringPrefix);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void batchLogwriter(long ts,String identifierData) throws Exception
//    {
//        if (counter<this.threshold)
//        {
//            System.out.print("data is inside hashmap");
//            batch.put(ts,tuple);
//            counter += 1;
//        }
//        else
//        {
//            //1-write Map to file
//
//            // create your filewriter and bufferedreader
////            fstream = new FileWriter("/home/ubuntu/values.csv",true);
//
//            Iterator<Map.Entry<Long, String>> it = batch.entrySet().iterator();
//            while (it.hasNext() )
//            {
//                // the key/value pair is stored here in pairs
//                Map.Entry<Long, String> pairs = it.next();
//                System.out.println("Value is " + pairs.getValue());
//
////                out.write("Timestamp :"+ pairs.getKey()+"\t value is : "+pairs.getValue() + "\n");
//                this.out.write( pairs.getKey()+","+pairs.getValue() + "\n");
////                out.write(pairs.getKey()+);
//            }
//            System.out.print("data is written to file");
//            this.out.flush();
//
//            //2-flush all data from map
//            batch.clear();
//
//            //3-insert new tuple to map
//            counter=1;
//            batch.put(ts,tuple);
//        }
//    }





//    public static void main(Strings args[])
//     {
//        BatchedFileLogging ba=new BatchedFileLogging();
//        for(int i=0;i<100;i++)
//            try {
//                ba.batchLogwriter(i,"entry isndoe file");
//            } catch (Exception e) {
//
//                System.out.println("Inside the Exception");
//                e.printStackTrace();
//            }
//         System.out.println("Inside the Exception");
//    }


//}
//*/
package in.dream_lab.bm.stream_iot.storm.genevents.logging;

//import in.dream_lab.genevents.utils.GlobalConstants;

import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by anshushukla on 20/05/15.
 */


public class BatchedFileLogging  {
    int counter=0;
//    Map<Long,String> batch=new HashMap<Long,String>();
    List<TupleType> batch = new ArrayList<TupleType>();
    FileWriter fstream;
    BufferedWriter out;
    String csvFileNameOut;
    int threshold; //Count of rows after which the map should be flushed to log file

    String logStringPrefix;

    public BatchedFileLogging(){
        this.csvFileNameOut = "/Users/anshushukla/data/values.csv";
    }

    public BatchedFileLogging(String csvFileNameOut, String componentName){
        this.csvFileNameOut = csvFileNameOut;
        try {
            this.fstream = new FileWriter(this.csvFileNameOut,false);
            this.out = new BufferedWriter(fstream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.threshold = GlobalConstants.thresholdFlushToLog; //2000 etc
        try {
            this.logStringPrefix = InetAddress.getLocalHost().getHostName() + "," + Thread.currentThread().getName() + "," + componentName;
            //System.out.println(this.logStringPrefix);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void batchLogwriter(long ts,String identifierData) throws Exception
    {
        if (counter<this.threshold)
        {
            //System.out.print("data is inside hashmap");
//            batch.put(ts,identifierData);
            batch.add(new TupleType(ts, identifierData));
            counter += 1;
        }
        else
        {
            //1-write Map to file
//            Iterator<Map.Entry<Long, String>> it = batch.entrySet().iterator();
//            while (it.hasNext() )
//            {
//                // the key/value pair is stored here in pairs
//                Map.Entry<Long, String> pairs = it.next();
//                //System.out.println("Value is " + pairs.getValue());
//
//                this.out.write( this.logStringPrefix + "," + pairs.getKey() + "," + pairs.getValue() + "\n");
//            }
//            System.out.print("data is written to file");
//            this.out.flush();
//
//            //2-flush all data from map
//            batch.clear();
//
//            //3-insert new tuple to map
//            counter=1;
//            batch.put(ts,identifierData);
            for(TupleType tp : batch){
                this.out.write( this.logStringPrefix + "," + tp.ts + "," + tp.identifier + "\n");
            }
            this.out.flush();

            batch.clear();

            counter = 1 ;
            batch.add(new TupleType(ts, identifierData));
        }
    }

//   public  void batchLogwriter(long ts,String tuple) throws Exception
//    {System.out.println("CALLED");
//        if (counter<this.threshold)
//        {
//            System.out.print("data is inside hashmap");
//            batch.put(ts,tuple);
//            counter += 1;
//        }
//        else
//        {
//            //1-write Map to file
//
//            // create your filewriter and bufferedreader
////            fstream = new FileWriter("/home/ubuntu/values.csv",true);
//
//            Iterator<Map.Entry<Long, String>> it = batch.entrySet().iterator();
//            while (it.hasNext() )
//            {
//                // the key/value pair is stored here in pairs
//                Map.Entry<Long, String> pairs = it.next();
//                System.out.println("Value is " + pairs.getValue());
//
////                out.write("Timestamp :"+ pairs.getKey()+"\t value is : "+pairs.getValue() + "\n");
//                this.out.write( pairs.getKey()+","+pairs.getValue() + "\n");
////                out.write(pairs.getKey()+);
//            }
//            System.out.print("data is written to file");
//            this.out.flush();
//
//            //2-flush all data from map
//            batch.clear();
//
//            //3-insert new tuple to map
//            counter=1;
//            batch.put(ts,tuple);
//        }
//    }





//    public static void main(Strings args[])
//     {
//        BatchedFileLogging ba=new BatchedFileLogging();
//        for(int i=0;i<100;i++)
//            try {
//                ba.batchLogwriter(i,"entry isndoe file");
//            } catch (Exception e) {
//
//                System.out.println("Inside the Exception");
//                e.printStackTrace();
//            }
//         System.out.println("Inside the Exception");
//    }


//TEMP  LOGIC
    //
    public static void writeToTemp(Object o,String csvFileNameOut)
    {

        //ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
//        String ExpNumber=GlobalConstants.getExperimentNumber();
//        System.out.println("ExpNumber-"+ExpNumber.split("-")[2]);

        System.out.println("from batchedfile logging");
        File temp = null;
//	String ExpNumber=GlobalConstants.getExperimentNumber();
        try {
            File f=new File("/tmp/nameWithPid");
            f.mkdirs();


                temp = File.createTempFile(o.getClass().getSimpleName()+"-"+System.currentTimeMillis()+"-"+csvFileNameOut.split("\\/")[csvFileNameOut.split("\\/").length-1]+
                        "-BoltPID-"+ ManagementFactory.getRuntimeMXBean().getName().split("@")[0]+"-", ".tmp",f);

            //write it
            System.out.println("TEMP FILE PATH"+temp);
            BufferedWriter tm = new BufferedWriter(new FileWriter(temp));
            String s="Hostname,"+ InetAddress.getLocalHost().getHostName()+",CurrentClassName,"+o.getClass().getSimpleName() +
                    ",BoltPID,"+ ManagementFactory.getRuntimeMXBean().getName()+",Timestamp,"+System.currentTimeMillis()+"\n";

            tm.write(s);
            tm.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("TEMP LOGIC Done");

    }

    public static void writeToTempDB(Object o,String csvFileNameOut,String conne)
    {

        //ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
//        String ExpNumber=GlobalConstants.getExperimentNumber();
//        System.out.println("ExpNumber-"+ExpNumber.split("-")[2]);

        System.out.println("from batchedfile logging");
        File temp = null;
//	String ExpNumber=GlobalConstants.getExperimentNumber();
        try {
            File f=new File("/tmp/nameWithPid");
            f.mkdirs();


            temp = File.createTempFile(o.getClass().getSimpleName()+"-"+System.currentTimeMillis()+"-"+csvFileNameOut.split("\\/")[csvFileNameOut.split("\\/").length-1]+
                    "-BoltPID-"+ ManagementFactory.getRuntimeMXBean().getName().split("@")[0]+"-", ".tmp",f);

            //write it
            System.out.println("TEMP FILE PATH"+temp);
            BufferedWriter tm = new BufferedWriter(new FileWriter(temp));
            String s="Hostname,"+conne+",CurrentClassName,"+o.getClass().getSimpleName() +
                    ",BoltPID,"+ ManagementFactory.getRuntimeMXBean().getName()+",Timestamp,"+System.currentTimeMillis()+"\n";

            tm.write(s);
            tm.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("TEMP LOGIC Done");

    }
    //

//

}


class TupleType{
    long ts;
    String identifier;

    public TupleType(long ts, String identifier){
        this.ts = ts;
        this.identifier = identifier;
    }
}
