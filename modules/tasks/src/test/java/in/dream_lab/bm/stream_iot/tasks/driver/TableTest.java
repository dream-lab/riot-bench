//package in.dream_lab.bm.stream_iot.tasks.driver;
//
//import org.junit.Assert;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import in.dream_lab.bm.stream_iot.tasks.io.AzureTableTask;
//
//import java.util.Properties;
//
///**
// * Created by anshushukla on 27/05/16.
// */
//public class TableTest extends AzureTableTask {
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
//        AzureTableTask az_table=new AzureTableTask();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//        az_table.setup(l,p_ );
//
////TODO: set useMsgField > 0
//        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        az_table.doTask(m);
//
////TODO: set useMsgField = -1
//
////        Random r=new Random();
////        for(int c=0;c<10;c++) {
////            String _rowkey = "" + r.nextInt(1220000);
////            l.warn(String.valueOf(az_table.doTask(_rowkey)));
////        }
//
//        az_table.tearDown();
//
//
//    }
//
//    @Test
//    public void testTableTest()
//    {
//
//        AzureTableTask az_table=new AzureTableTask();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//        az_table.setup(l,p_ );
//
////TODO: set useMsgField > 0
//        String m="1,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
//        Float tableResult = az_table.doTask(m);  //  check not equal to -1
//
//
////TODO: set useMsgField = -1
//
////        Random r=new Random();
////        for(int c=0;c<10;c++) {
////            String _rowkey = "" + r.nextInt(1220000);
////            l.warn(String.valueOf(az_table.doTask(_rowkey)));
////        }
//
//        Assert.assertTrue(tableResult!=-1); // assuming table entry output is not -1
//        az_table.tearDown();
//
//    }
//}
