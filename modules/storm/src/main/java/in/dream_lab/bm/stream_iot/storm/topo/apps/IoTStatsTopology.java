package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */

import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;


/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTStatsTopology {

    public static void main(String[] args) throws Exception {

        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
        String taskPropFilename=argumentClass.getTasksPropertiesFilename();
        System.out.println("taskPropFilename-"+taskPropFilename);


        Config conf = new Config();
        conf.setDebug(true);
        //conf.setNumWorkers(12);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);


        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new SampleSpout(argumentClass.getInputDatasetPathName(), spoutLogFileName, argumentClass.getScalingFactor()),
                1);

        builder.setBolt("ParseProjectSYSBolt",
                new ParseProjectSYSBolt(p_), 1)
                .shuffleGrouping("spout");

        builder.setBolt("BloomFilterCheckBolt",
                new BloomFilterCheckBolt(p_), 1)
                .fieldsGrouping("ParseProjectSYSBolt",new Fields("obsType")); // filed grouping on obstype

        builder.setBolt("KalmanFilterBolt",
                new KalmanFilterBolt(p_), 1)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("sensorID","obsType"));

        builder.setBolt("SimpleLinearRegressionPredictorBolt",
                new SimpleLinearRegressionPredictorBolt(p_), 1)
                .fieldsGrouping("KalmanFilterBolt",new Fields("sensorID","obsType"));

        builder.setBolt("SecondOrderMomentBolt",
                new SecondOrderMomentBolt(p_), 1)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("sensorID","obsType"));

        builder.setBolt("DistinctApproxCountBolt",
                new DistinctApproxCountBolt(p_), 1)
                .fieldsGrouping("BloomFilterCheckBolt",new Fields("obsType")); // another change done already

        builder.setBolt("MQTTPublishTaskBolt",
                new MQTTPublishTaskBolt(p_), 1)
                .shuffleGrouping("SimpleLinearRegressionPredictorBolt")
                .shuffleGrouping("SecondOrderMomentBolt")
                .shuffleGrouping("DistinctApproxCountBolt");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("MQTTPublishTaskBolt");


        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(100000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
    }
}


// L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

// L   IdentityTopology   /Users/anshushukla/data/dataset-SYS-12min-100x.csv     SYS-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test

// L   IdentityTopology   /Users/anshushukla/data/dataset-TAXI-12min-100x.csv     TAXI-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties  test