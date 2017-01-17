package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */


import in.dream_lab.bm.stream_iot.storm.bolts.TRAIN.FIT.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpoutTimerForTrain;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

//import in.dream_lab.bm.stream_iot.storm.spouts.TimeSpout;


/**
 * Created by anshushukla on 18/05/15.
 */
public class IoTTrainTopologyFIT {

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
        conf.setDebug(false);
        conf.put("topology.backpressure.enable",false);
        conf.setNumWorkers(8);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);



        TopologyBuilder builder = new TopologyBuilder();


        String basePathForMultipleSpout="/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/";
//        String basePathForMultipleSpout="/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/";

        System.out.println("basePathForMultipleSpout is used -"+basePathForMultipleSpout);

        String spout1InputFilePath=basePathForMultipleSpout+ "inputFileForTimerSpout-FIT.csv";

//        String spout1InputFilePath=basePathForMultipleSpout+ "inputFileForTimerSpout-TAXI-train-microbench-1min.csv";

        builder.setSpout("TimeSpout", new SampleSpoutTimerForTrain(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);


//        builder.setSpout("TimeSpout", new TimeSpout(),1);

        builder.setBolt("AzureTableRangeQueryBolt",
                new AzureTableRangeQueryBolt(p_), 1)
                .shuffleGrouping("TimeSpout");




        builder.setBolt("LinearRegressionTrainBolt",
                new LinearRegressionTrainBolt(p_), 1)
                .shuffleGrouping("AzureTableRangeQueryBolt");


        builder.setBolt("AzureBlobUploadTaskBolt",
                new AzureBlobUploadTaskBolt(p_), 1)
                .shuffleGrouping("DecisionTreeTrainBolt")
                .shuffleGrouping("LinearRegressionTrainBolt");

        builder.setBolt("MQTTPublishBolt",
                new MQTTPublishBolt(p_), 1)
                .shuffleGrouping("AzureBlobUploadTaskBolt");


        builder.setBolt("AnnotateDTClassBolt",
                new AnnotateDTClassBolt(p_), 1)
                .shuffleGrouping("AzureTableRangeQueryBolt");

        builder.setBolt("DecisionTreeTrainBolt",
                new DecisionTreeTrainBolt(p_), 1)
                .shuffleGrouping("AnnotateDTClassBolt");


        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("MQTTPublishBolt");

//        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("LinearRegressionTrainBolt");

//*/

        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(1000000000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
    }
}

//    L   IdentityTopology  /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsv-predict-10spouts600mps-480sec-file/SYS-inputcsv-predict-10spouts600mps-480sec-file1.csv     SYS-210  0.001   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_TAXI.properties  test