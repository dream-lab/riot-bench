package in.dream_lab.bm.stream_iot.storm.topo.micro;

import in.dream_lab.bm.stream_iot.storm.bolts.AggregateBolts;
import in.dream_lab.bm.stream_iot.storm.bolts.BaseTaskBolt;
import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;

import java.util.Properties;

public class MicroTopologyFactory {

	public static BaseTaskBolt newTaskBolt(String taskName, Properties p) {
		switch(taskName) {
			//aggregate
			case "BlockWindowAverage" : return newBlockWindowAverageBolt(p); 
			case "DistinctApproxCount" : return newDistinctApproxCountBolt(p);
			case "Accumlator" : return newAccumlatorBolt(p);
			
			//filter
			case "BloomFilterCheck" : return newBloomFilterCheckBolt(p);
			case "BloomFilterTrain" : return newBloomFilterTrainBolt(p);
			case "RangeFilterCheck" : return newRangeFilterCheck(p);
			//io
			case "AzureBlobDownload" : return newAzureBlobDownloadTaskBolt(p);
			case "AzureBlobUpload" : return newAzureBlobUploadTaskBolt(p);
			case "AzureTable" : return newAzureTableTaskBolt(p);
			case "AzureWrite" : return newAzureTableInsertBolt(p);
			case "MQTTPublish" : return newMQTTPublishTaskBolt(p);
			case "ZipMultipleBuffer" : return newZipMultipleBuffer(p);
			case "LinearRegressionTrainBatched" : return newLinearRegressionTrainBatched(p);
			case "DecisionTreeTrainBatched" : return newDecisionTreeTrainBatched(p);
			
			//math
			case "PiByViete" : return newPiByVieteBolt(p);
			//parse
			case "XMLParse" : return newXMLParseBolt(p);
			case "SenMlParse" : return newSenMLParse(p); 
			case "CsvToSenML" :	return newCsvToSenMlParse(p);	
			//predict
			case "DecisionTreeClassify" : return newDecisionTreeClassifyBolt(p);
			case "DecisionTreeTrain" : return newDecisionTreeTrainBolt(p);
			case "LinearRegressionPredictor" : return newLinearRegressionPredictorBolt(p);
			case "LinearRegressionTrain" : return newLinearRegressionTrainBolt(p);
			case "SimpleLinearRegressionPredictor" : return newSimpleLinearRegressionPredictorBolt(p);
			//statistics
			case "KalmanFilter" : return newKalmanFilterBolt(p);
			case "SecondOrderMoment" : return newSecondOrderMomentBolt(p);
			case "Interpolation" : return newInterpolationBolt(p);
			//annotate
			case "Annotate" : return newAnnotateBolt(p);
			
			//no operation task for benchmark
			case "NoOperation" : return newNoOperationBolt(p); 
			
			//visualize
			case "LineChartPlot" :  return newLineChartPlotBolt(p);
			case "MultiLineChartPlot" : return newMultiLineChartPlotBolt(p); 
			
			default: throw new IllegalArgumentException("Unknown class name for bolt/task: " + taskName);
		}
		
	}

	public static BaseTaskBolt newBlockWindowAverageBolt(Properties p) {
		return new AggregateBolts.BlockWindowAverageBolt(p);
	}

	public static BaseTaskBolt newDistinctApproxCountBolt(Properties p) {
		return new AggregateBolts.DistinctApproxCountBolt(p);
	}
	public static BaseTaskBolt newAccumlatorBolt(Properties p)
	{
		return new AggregateBolts.AccumlatorBolt(p);
	}
	

	//filter
	public static BaseTaskBolt newBloomFilterCheckBolt(Properties p) {
		return new AggregateBolts.BloomFilterCheckBolt(p);
	}
	public static BaseTaskBolt newBloomFilterTrainBolt(Properties p) {
		return new AggregateBolts.BloomFilterTrainBolt(p);
	}

	public static BaseTaskBolt newRangeFilterCheck(Properties p) {
		return new AggregateBolts.RangeFilterCheckBolt(p);
	} 
	//io
	public static BaseTaskBolt newAzureBlobDownloadTaskBolt(Properties p) {
		return new AggregateBolts.AzureBlobDownloadTaskBolt(p);
	}
	public static BaseTaskBolt newAzureBlobUploadTaskBolt(Properties p) {
		return new AggregateBolts.AzureBlobUploadTaskBolt(p);
	}
	public static BaseTaskBolt newAzureTableTaskBolt(Properties p) {
		return new AggregateBolts.AzureTableTaskBolt(p);
	}
	public static BaseTaskBolt newMQTTPublishTaskBolt(Properties p) {
		return new AggregateBolts.MQTTPublishTaskBolt(p);
	}
	public static BaseTaskBolt newAzureTableInsertBolt (Properties p) {
		return new AggregateBolts.AzureTableInsertBolt(p);
	}
	public static BaseTaskBolt newZipMultipleBuffer(Properties p)
	{
		return new AggregateBolts.ZipMultipleBufferBolt(p);
	}
	public static BaseTaskBolt newLinearRegressionTrainBatched(Properties p)
	{
		return new AggregateBolts.LinearRegressionTrainBatchedBolt(p);
	}
	public static BaseTaskBolt newDecisionTreeTrainBatched(Properties p)
	{
		return new AggregateBolts.DecisionTreeTrainBatchedBolt(p);
	}
	//math
	public static BaseTaskBolt newPiByVieteBolt(Properties p) {
		return new AggregateBolts.PiByVieteBolt(p);
	}

	//parse
	public static BaseTaskBolt newXMLParseBolt(Properties p) {
		return new AggregateBolts.XMLParseBolt(p);
	}
	
	public static BaseTaskBolt newSenMLParse(Properties p) {
		return new AggregateBolts.SenMlParseBolt(p);
	}
	
	public static BaseTaskBolt newCsvToSenMlParse(Properties p) {
		return new AggregateBolts.CsvToSenMlParseBolt(p);
	}
	
	//predict
	public static BaseTaskBolt newDecisionTreeClassifyBolt(Properties p) {
		return new AggregateBolts.DecisionTreeClassifyBolt(p);
	}
	public static BaseTaskBolt newDecisionTreeTrainBolt(Properties p) {
		return new AggregateBolts.DecisionTreeTrainBolt(p);
	}
	public static BaseTaskBolt newLinearRegressionPredictorBolt(Properties p) {
		return new AggregateBolts.LinearRegressionPredictorBolt(p);
	}
	public static BaseTaskBolt newLinearRegressionTrainBolt(Properties p) {
		return new AggregateBolts.LinearRegressionTrainBolt(p);
	}
	public static BaseTaskBolt newSimpleLinearRegressionPredictorBolt(Properties p) {
		return new AggregateBolts.SimpleLinearRegressionPredictorBolt(p);
	}

	//statistics
	public static BaseTaskBolt newKalmanFilterBolt(Properties p) {
		return new AggregateBolts.KalmanFilterBolt(p);
	}
	public static BaseTaskBolt newSecondOrderMomentBolt(Properties p) {
		return new AggregateBolts.SecondOrderMomentBolt(p);
	}
	public static BaseTaskBolt newInterpolationBolt(Properties p) {
		return new AggregateBolts.InterpolationBolt(p);
	}
	//annotate
	public static BaseTaskBolt newAnnotateBolt(Properties p) {
		return new AggregateBolts.AnnotateBolt(p);
	}
	
	//no operation 
	public static BaseTaskBolt newNoOperationBolt(Properties p)
	{
		return new AggregateBolts.NoOperationBolt(p);
	}
	
	//visualize
	public static BaseTaskBolt newLineChartPlotBolt(Properties p)
	{
		return new AggregateBolts.LineChartPlotBolt(p);
	}
	
	public static BaseTaskBolt newMultiLineChartPlotBolt(Properties p)
	{
		return new AggregateBolts.MultiLineChartPlotBolt(p);
	}
}


//	L   IdentityTopology   /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv     PLUG-210  1.0   /Users/anshushukla/data/output/temp    /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties    BlockWindowAverage