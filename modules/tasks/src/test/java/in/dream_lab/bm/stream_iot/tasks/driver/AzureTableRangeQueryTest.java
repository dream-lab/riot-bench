package in.dream_lab.bm.stream_iot.tasks.driver;

import com.google.common.primitives.Doubles;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobDownloadTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableRangeQueryTaskSYS;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Created by anshushukla on 27/05/16.
 */
public class AzureTableRangeQueryTest extends AzureBlobDownloadTask {

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
        AzureTableRangeQueryTaskSYS az_tableRange=new AzureTableRangeQueryTaskSYS();
        
        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        System.out.println("Table test is working ********** ");
        p_.load(new FileReader("/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties"));
        az_tableRange.setup(l,p_ );
//        String fileIndex="peakRateBarplot.pdf";

        HashMap<String, String> map = new HashMap();
        map.put("ROWKEYSTART", "3629200");
        map.put("ROWKEYEND", "3629300");
        
        Float size = az_tableRange.doTask(map);
//        Assert.assertEquals(az_blob.doTask(fileIndex), "Hello World");
    Iterable<AzureTableRangeQueryTaskSYS.TaxiDropoffEntity> result= (Iterable<AzureTableRangeQueryTaskSYS.TaxiDropoffEntity>) az_tableRange.getLastResult();



    List<Double> doubleValues = new ArrayList<>();

    // Loop through the results, displaying information about the entity
    for (AzureTableRangeQueryTaskSYS.TaxiDropoffEntity entity : result) {
        System.out.println(entity.getPartitionKey() + " " + entity.getRowKey() + "\t" + entity.getFare_amount());
        doubleValues.add(Double.valueOf(entity.getFare_amount()));
    }

    double[] data = Doubles.toArray(doubleValues);
    // The data must be ordered
    Arrays.sort(data);
    Percentile percentile = new Percentile();
    percentile.setData(data);

    System.out.println("percentile results:"+percentile.evaluate(100));







    az_tableRange.tearDown();
}
catch(Exception e)
{
}

    }

    @Test
    public void testBlob()
    {
        AzureTableRangeQueryTaskSYS az_tableRange=new AzureTableRangeQueryTaskSYS();

        initLogger(LoggerFactory.getLogger("APP"));
        Properties p_=new Properties();
        System.out.println("blob test is working ********** ");

        az_tableRange.setup(l,p_ );
//        String fileIndex="5,1443033000,ci4ue1845000102w7ni64j7pl,-71.106167,42.372802,-0.1,65.3,0,367.38,26";
        
        HashMap<String, String> map = new HashMap();
        map.put("ROWKEYSTART", "1");
        map.put("ROWKEYEND", "10000");
        
        Float size = az_tableRange.doTask(map);
        System.out.println("Size of blob is "+size);

        az_tableRange.tearDown();
        assertTrue(size>0);
    }
}

