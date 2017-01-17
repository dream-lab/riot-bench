//package in.dream_lab.bm.stream_iot.tasks.driver;
//
//import org.junit.Assert;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import in.dream_lab.bm.stream_iot.tasks.parse.XMLParse;
//
//import java.util.Properties;
//
///**
// * Created by anshushukla on 27/05/16.
// */
//public class XMLparsingTest extends XMLParse {
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
//        XMLParse l1=new XMLParse();
//
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//
//
//        l1.setup(l,p_ );
//
//
//        l1.doTask("dummy");
//
//        l.warn(String.valueOf(l1.tearDown()));
//
//
//    }
//
//    @Test
//    public void testXMLparsingTest()
//    {
//        XMLParse l1=new XMLParse();
//        initLogger(LoggerFactory.getLogger("APP"));
//        Properties p_=new Properties();
//        l1.setup(l,p_ );
//
//        Float parseFileLength = l1.doTask("dummy");
//        Assert.assertTrue(parseFileLength>0);
//        l.warn(String.valueOf(l1.tearDown()));
//
//    }
//}
