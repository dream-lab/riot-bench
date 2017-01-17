package in.dream_lab.bm.stream_iot.storm.bolts;

import in.dream_lab.bm.stream_iot.tasks.ITask;
import in.dream_lab.bm.stream_iot.tasks.NoOperationTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.AccumlatorTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.BlockWindowAverage;
import in.dream_lab.bm.stream_iot.tasks.aggregate.DistinctApproxCount;
import in.dream_lab.bm.stream_iot.tasks.annotate.Annotate;
import in.dream_lab.bm.stream_iot.tasks.filter.BloomFilterCheck;
import in.dream_lab.bm.stream_iot.tasks.filter.BloomFilterTrain;
import in.dream_lab.bm.stream_iot.tasks.filter.RangeFilterCheck;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobDownloadTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobUploadTask;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableInsert;
import in.dream_lab.bm.stream_iot.tasks.io.AzureTableTask;
import in.dream_lab.bm.stream_iot.tasks.io.MQTTPublishTask;
import in.dream_lab.bm.stream_iot.tasks.io.ZipMultipleBufferTask;
import in.dream_lab.bm.stream_iot.tasks.math.PiByViete;
import in.dream_lab.bm.stream_iot.tasks.parse.CsvToSenMLParse;
import in.dream_lab.bm.stream_iot.tasks.parse.SenMLParse;
import in.dream_lab.bm.stream_iot.tasks.parse.XMLParse;
import in.dream_lab.bm.stream_iot.tasks.predict.*;
import in.dream_lab.bm.stream_iot.tasks.statistics.Interpolation;
import in.dream_lab.bm.stream_iot.tasks.statistics.KalmanFilter;
import in.dream_lab.bm.stream_iot.tasks.statistics.SecondOrderMoment;
import in.dream_lab.bm.stream_iot.tasks.visualize.XChartLinePlotTask;
import in.dream_lab.bm.stream_iot.tasks.visualize.XChartMultiLinePlotTask;

import java.util.Properties;

public class AggregateBolts {
	public static class BlockWindowAverageBolt extends BaseTaskBolt {
	    public BlockWindowAverageBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new BlockWindowAverage(); }
	}
	
	public static class DistinctApproxCountBolt extends BaseTaskBolt {
	    public DistinctApproxCountBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new DistinctApproxCount(); }
	}
	public static class AccumlatorBolt extends BaseTaskBolt {
	    public AccumlatorBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new AccumlatorTask(); }
	}
	
	//filter

	public static class BloomFilterCheckBolt extends BaseTaskBolt {
		public BloomFilterCheckBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new BloomFilterCheck();
		}
	}
	public static class BloomFilterTrainBolt extends BaseTaskBolt {
		public BloomFilterTrainBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new BloomFilterTrain();
		}
	}
	
	public static class RangeFilterCheckBolt extends BaseTaskBolt 
	{
		public RangeFilterCheckBolt(Properties p_) { super(p_); }
		
		@Override
		protected ITask getTaskInstance() { return new RangeFilterCheck();
		}
	}
	//io

	public static class AzureBlobDownloadTaskBolt extends BaseTaskBolt {
		public AzureBlobDownloadTaskBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new AzureBlobDownloadTask();
		}
	}
	public static class AzureBlobUploadTaskBolt extends BaseTaskBolt {
		public AzureBlobUploadTaskBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new AzureBlobUploadTask();
		}
	}
	public static class AzureTableTaskBolt extends BaseTaskBolt {
		public AzureTableTaskBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new AzureTableTask();
		}
	}
	public static class MQTTPublishTaskBolt extends BaseTaskBolt {
		public MQTTPublishTaskBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new MQTTPublishTask();
		}
	}
	
	public static class AzureTableInsertBolt extends BaseTaskBolt {
		public AzureTableInsertBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new AzureTableInsert();
		}
	}
	public static class ZipMultipleBufferBolt extends BaseTaskBolt {
	    public ZipMultipleBufferBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new ZipMultipleBufferTask(); }
	}
	public static class LinearRegressionTrainBatchedBolt extends BaseTaskBolt {
		public LinearRegressionTrainBatchedBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new LinearRegressionTrainBatched(); }
	}
	public static class DecisionTreeTrainBatchedBolt extends BaseTaskBolt {
		public DecisionTreeTrainBatchedBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new DecisionTreeTrainBatched(); }
	}


	//math
	public static class PiByVieteBolt extends BaseTaskBolt {
		public PiByVieteBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new PiByViete();
		}
	}

	//parse
	public static class XMLParseBolt extends BaseTaskBolt {
		public XMLParseBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new XMLParse();
		}
	}
	
	public static class SenMlParseBolt extends BaseTaskBolt {
		public SenMlParseBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new SenMLParse();
		}
	}

	public static class CsvToSenMlParseBolt extends BaseTaskBolt {
		public CsvToSenMlParseBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new CsvToSenMLParse();
		}
	}

	
	//predict
	public static class DecisionTreeClassifyBolt extends BaseTaskBolt {
		public DecisionTreeClassifyBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new DecisionTreeClassify();
		}
	}
	public static class DecisionTreeTrainBolt extends BaseTaskBolt {
		public DecisionTreeTrainBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new DecisionTreeTrain();
		}
	}
	public static class LinearRegressionPredictorBolt extends BaseTaskBolt {
		public LinearRegressionPredictorBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new LinearRegressionPredictor();
		}
	}
	public static class LinearRegressionTrainBolt extends BaseTaskBolt {
		public LinearRegressionTrainBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new LinearRegressionTrain();
		}
	}
	public static class SimpleLinearRegressionPredictorBolt extends BaseTaskBolt {
		public SimpleLinearRegressionPredictorBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new SimpleLinearRegressionPredictor();
		}
	}
	//statistics
	public static class KalmanFilterBolt extends BaseTaskBolt {
		public KalmanFilterBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new KalmanFilter();
		}
	}
	public static class SecondOrderMomentBolt extends BaseTaskBolt {
		public SecondOrderMomentBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new SecondOrderMoment();
		}
	}
	
	public static class InterpolationBolt extends BaseTaskBolt {
		public InterpolationBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new Interpolation();
		}
	}

	//Annotate
	public static class AnnotateBolt extends BaseTaskBolt {
		public AnnotateBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new Annotate();
		}
	}
	//no operation
	public static class NoOperationBolt extends BaseTaskBolt {
		public NoOperationBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new NoOperationTask();
		}
	}
	
	//visualize 
	
	public static class LineChartPlotBolt extends BaseTaskBolt {
		public LineChartPlotBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new XChartLinePlotTask();
		}
	}
	
	public static class MultiLineChartPlotBolt extends BaseTaskBolt {
		public MultiLineChartPlotBolt(Properties p_) { super(p_); }

		@Override
		protected ITask getTaskInstance() { return new XChartMultiLinePlotTask();
		}
	}
}
