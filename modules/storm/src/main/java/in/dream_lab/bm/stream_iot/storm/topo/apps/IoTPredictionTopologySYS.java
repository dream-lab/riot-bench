package in.dream_lab.bm.stream_iot.storm.topo.apps;

/**
 * Created by anshushukla on 03/06/16.
 */


import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SenMLParseBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.*;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.MQTTSubscribeSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
//import in.dream_lab.bm.stream_iot.storm.spouts.TimeSpout;
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
public class IoTPredictionTopologySYS {

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
        //conf.setNumWorkers(12);


        Properties p_=new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);



        TopologyBuilder builder = new TopologyBuilder();


//        String basePathForMultipleSpout="/Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsv-10spouts600mps-480sec-file/";
        String basePathForMultipleSpout="/Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/";

//        String basePathForMultipleSpout="/home/anshu/data/storm/dataset/SYS-splitted-data/";

        System.out.println("basePathForMultipleSpout is used -"+basePathForMultipleSpout);

        String spout1InputFilePath=basePathForMultipleSpout+"SYS_sample_data_senml.csv";
//        String spout1InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file1.csv";
//        String spout2InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file2.csv";
//        String spout3InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file3.csv";
//        String spout4InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file4.csv";
//        String spout5InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file5.csv";
//        String spout6InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file6.csv";
//        String spout7InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file7.csv";
//        String spout8InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file8.csv";
//        String spout9InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file9.csv";
//        String spout10InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts600mps-480sec-file10.csv";

        builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
                1);
//        builder.setSpout("spout2", new SampleSenMLSpout(spout2InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout3", new SampleSenMLSpout(spout3InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout4", new SampleSenMLSpout(spout4InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout5", new SampleSenMLSpout(spout5InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout6", new SampleSenMLSpout(spout6InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout7", new SampleSenMLSpout(spout7InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout8", new SampleSenMLSpout(spout8InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout9", new SampleSenMLSpout(spout9InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);
//        builder.setSpout("spout10", new SampleSenMLSpout(spout10InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//                1);

        builder.setBolt("SenMLParseBoltPREDSYS",
                new SenMLParseBoltPREDSYS(p_), 1)
                .shuffleGrouping("spout1")
//                 	.shuffleGrouping("spout2")
//         			.shuffleGrouping("spout3")
//         			.shuffleGrouping("spout4")
//         			.shuffleGrouping("spout5")
//         			.shuffleGrouping("spout6")
//         			.shuffleGrouping("spout7")
//         			.shuffleGrouping("spout8")
//         			.shuffleGrouping("spout9")
//		            .shuffleGrouping("spout10");
        ;



//

        builder.setSpout("mqttSubscribeTaskBolt",
                new MQTTSubscribeSpout(p_,"dummyLog-SYS"), 1); // "RowString" should have path of blob

        builder.setBolt("AzureBlobDownloadTaskBolt",
                new AzureBlobDownloadTaskBolt(p_), 1)
                .shuffleGrouping("mqttSubscribeTaskBolt");




        builder.setBolt("DecisionTreeClassifyBolt",
                new DecisionTreeClassifyBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDSYS")
                .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));
//
//
        builder.setBolt("LinearRegressionPredictorBolt",
                new LinearRegressionPredictorBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDSYS")
                .fieldsGrouping("AzureBlobDownloadTaskBolt",new Fields("ANALAYTICTYPE"));


        builder.setBolt("BlockWindowAverageBolt",
                new BlockWindowAverageBolt(p_), 1)
                .shuffleGrouping("SenMLParseBoltPREDSYS");

//
        builder.setBolt("ErrorEstimationBolt",
                new ErrorEstimationBolt(p_), 1)
                .shuffleGrouping("BlockWindowAverageBolt")
                .shuffleGrouping("LinearRegressionPredictorBolt");

        builder.setBolt("MQTTPublishBolt",
                new MQTTPublishBolt(p_), 1)
                .fieldsGrouping("ErrorEstimationBolt",new Fields("ANALAYTICTYPE"))
                .fieldsGrouping("DecisionTreeClassifyBolt",new Fields("ANALAYTICTYPE")) ;


        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping("MQTTPublishBolt");



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


//    L   IdentityTopology  /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsv-predict-10spouts600mps-480sec-file/SYS-inputcsv-predict-10spouts600mps-480sec-file1.csv     SYS-210  0.001   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_CITY.properties  test