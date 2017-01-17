package in.dream_lab.bm.stream_iot.tasks.driver;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobDownloadTask;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertTrue;

/**
 * Created by anshushukla on 27/05/16.
 */
public class BlobDownloadTest extends AzureBlobDownloadTask {

//    Float size;
    private static Logger l; // TODO: Ensure logger is initialized before use

    /**
     *
     * @param l_
     */
    public static void initLogger(Logger l_) {
        l = l_;
    }

    public static void main(String[] args) {

try {
        AzureBlobDownloadTask az_blob=new AzureBlobDownloadTask();
        
        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        System.out.println("blob test is working ********** ");
        p_.load(new FileReader("/home/shilpa/Sandbox/Repository/bmIOT/bm-iot/modules/tasks/src/main/resources/tasks.properties"));
        az_blob.setup(l,p_ );
        String fileIndex="peakRateBarplot.pdf";
        
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, fileIndex);
        
        Float size = az_blob.doTask(map);
//        Assert.assertEquals(az_blob.doTask(fileIndex), "Hello World");

        az_blob.tearDown();
}
catch(Exception e)
{
}

    }

    @Test
    public void testBlob()
    {
        AzureBlobDownloadTask az_blob=new AzureBlobDownloadTask();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        System.out.println("blob test is working ********** ");

        az_blob.setup(l,p_ );
        String fileIndex="5,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        
        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, fileIndex);
        
        Float size = az_blob.doTask(map);
        System.out.println("Size of blob is "+size);

        az_blob.tearDown();
        assertTrue(size>0);
    }
}

