package in.dream_lab.bm.stream_iot.storm.topo.apps;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.AnnotationBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.AzureTableInsertBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.BloomFilterCheckBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.CsvToSenMLBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.InterpolationBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.JoinBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.MQTTPublishBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.RangeFilterBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SQLTableInsertBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.ETL.TAXI.SenMLParseBolt;
import in.dream_lab.bm.stream_iot.storm.bolts.IoTPredictionBolts.SYS.LinearRegressionPredictorBolt;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSenMLSpout;

public class ETL_SQL_Topology 
{
	 public static void main(String[] args) throws Exception
	 {
		 ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
		 if (argumentClass == null) {
			 System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
			 return;
		 }
		 String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-" + argumentClass.getScalingFactor() + ".log";
		 String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
		 String spoutLogFileName = argumentClass.getOutputDirName() + "/spout-" + logFilePrefix;
		 String taskPropFilename=argumentClass.getTasksPropertiesFilename();
		 String spout1InputFilePath=argumentClass.getInputDatasetPathName();
		 
		 Config conf = new Config();
		 conf.setDebug(false);
		 Properties p_=new Properties();
		 InputStream input = new FileInputStream(taskPropFilename);
		 p_.load(input);
		 TopologyBuilder builder = new TopologyBuilder();
		
		/*The below code shows how we can have multiple spouts read from different files 
		This is to provide multiple spout threads running at the same time but reading 
		data from separate file - Shilpa  */

	    
	   
//       String spout2InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file2.csv";
//       String spout3InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file3.csv";
//
//       String spout4InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file4.csv";
//       String spout5InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file5.csv";
//       String spout6InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file6.csv";
//       String spout7InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file7.csv";
//       String spout8InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file8.csv";
//       String spout9InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file9.csv";
//       String spout10InputFilePath=basePathForMultipleSpout+"SYS-inputcsv-predict-10spouts200mps-480sec-file10.csv";
		


        builder.setSpout("spout1", new SampleSenMLSpout(spout1InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
               1);
//       builder.setSpout("spout2", new SampleSenMLSpout(spout2InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout3", new SampleSenMLSpout(spout3InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout4", new SampleSenMLSpout(spout4InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout5", new SampleSenMLSpout(spout5InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout6", new SampleSenMLSpout(spout6InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout7", new SampleSenMLSpout(spout7InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout8", new SampleSenMLSpout(spout8InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout9", new SampleSenMLSpout(spout9InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
//       builder.setSpout("spout10", new SampleSenMLSpout(spout10InputFilePath, spoutLogFileName, argumentClass.getScalingFactor()),
//               1);
		 
	   builder.setBolt("SenMlParseBolt",
	                new SenMLParseBolt(p_), 1)
	                .shuffleGrouping("spout1");
////                 	.shuffleGrouping("spout2")
//         			.shuffleGrouping("spout3")
//         			.shuffleGrouping("spout4")
//         			.shuffleGrouping("spout5")
//         			.shuffleGrouping("spout6")
//         			.shuffleGrouping("spout7")
//         			.shuffleGrouping("spout8")
//         			.shuffleGrouping("spout9")
//		            .shuffleGrouping("spout10");


        builder.setBolt("RangeFilterBolt",
	                new RangeFilterBolt(p_), 1)
	                .fieldsGrouping("SenMlParseBolt", new Fields("OBSTYPE"));

		 builder.setBolt("BloomFilterBolt",
	                new BloomFilterCheckBolt(p_), 1)
	                .fieldsGrouping("RangeFilterBolt", new Fields("OBSTYPE"));
		 
		 builder.setBolt("InterpolationBolt",
	                new InterpolationBolt(p_), 1)
	                .fieldsGrouping("BloomFilterBolt", new Fields("OBSTYPE"));
		 
		 builder.setBolt("JoinBolt",
	                new JoinBolt(p_), 1)
	                .fieldsGrouping("InterpolationBolt", new Fields("MSGID"));
		 
		 builder.setBolt("AnnotationBolt",
	                new AnnotationBolt(p_), 1)
	                .shuffleGrouping("JoinBolt"); 

		 builder.setBolt("SQLInsert",
	                new SQLTableInsertBolt(p_), 1)
	                .shuffleGrouping("AnnotationBolt");
		 
		 builder.setBolt("CsvToSenMLBolt",
	                new CsvToSenMLBolt(p_), 1)
	                .shuffleGrouping("AnnotationBolt");
 
		 builder.setBolt("PublishBolt",
	                new MQTTPublishBolt(p_), 1)
	                .shuffleGrouping("CsvToSenMLBolt");

		 builder.setBolt("sink", new Sink(sinkLogFileName), 1)
         			.shuffleGrouping("PublishBolt")
		            .shuffleGrouping("SQLInsert");
		 
		 StormTopology stormTopology = builder.createTopology();
		 
		 if (argumentClass.getDeploymentMode().equals("C")) 
		 {
	            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
	        } else {
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
	            Utils.sleep(900000);
	            cluster.killTopology(argumentClass.getTopoName());
	            cluster.shutdown();
	        }
	 }
}
