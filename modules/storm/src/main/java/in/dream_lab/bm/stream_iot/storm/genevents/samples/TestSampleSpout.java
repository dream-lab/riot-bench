package in.dream_lab.bm.stream_iot.storm.genevents.samples;



import in.dream_lab.bm.stream_iot.storm.genevents.EventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.ISyntheticEventGen;
import in.dream_lab.bm.stream_iot.storm.genevents.logging.BatchedFileLogging;
import in.dream_lab.bm.stream_iot.storm.genevents.utils.GlobalConstants;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TestSampleSpout extends BaseRichSpout implements ISyntheticEventGen {
	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;

	public TestSampleSpout(){
//		this.csvFileName = "/home/ubuntu/sample100_sense.csv";
//		System.out.println("Inside  sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
//		System.out.print("the output is as follows");
	}

	public TestSampleSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public TestSampleSpout(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
		this(csvFileName, outSpoutCSVLogFileName, scalingFactor, "");
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
//		try {
//		System.out.println("spout Queue count= "+this.eventQueue.size());
		// allow multiple tuples to be emitted per next tuple.
		// Discouraged? https://groups.google.com/forum/#!topic/storm-user/SGwih7vPiDE
		int count = 0, MAX_COUNT=10; // FIXME?
		while(count < MAX_COUNT) {
			List<String> entry = this.eventQueue.poll(); // nextTuple should not block!
			if(entry == null) return;
			count++;
			Values values = new Values();
			StringBuilder rowStringBuf = new StringBuilder();
			for(String s : entry){
				rowStringBuf.append(",").append(s);
			}
			String rowString = rowStringBuf.toString().substring(1);
			values.add(rowString);
			msgId++;
			values.add(Long.toString(msgId));
			this._collector.emit(values);
			try {
//				msgId++;
				ba.batchLogwriter(System.currentTimeMillis(),"MSGID," + msgId);
				//ba.batchLogwriter(System.nanoTime(),"MSGID," + msgId);
			} catch (Exception e) {
				e.printStackTrace();
			}
//			System.out.println("values by source are -" + values);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		}
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
//		System.out.println("SampleSpout PID,"+ ManagementFactory.getRuntimeMXBean().getName());
		BatchedFileLogging.writeToTemp(this,this.outSpoutCSVLogFileName);
		Random r=new Random();


		try {
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup1")==0)
				msgId= (long) (1*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup2")==0)
				msgId= (long) (2*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup3")==0)
				msgId= (long) (3*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup4")==0)
				msgId= (long) (4*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup5")==0)
				msgId= (long) (5*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup6")==0)
				msgId= (long) (6*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup7")==0)
				msgId= (long) (7*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup8")==0)
				msgId= (long) (8*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup9")==0)
				msgId= (long) (9*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup10")==0)
				msgId= (long) (10*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup11")==0)
				msgId= (long) (11*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup12")==0)
				msgId= (long) (12*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup13")==0)
				msgId= (long) (13*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup14")==0)
				msgId= (long) (14*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup15")==0)
				msgId= (long) (15*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup16")==0)
				msgId= (long) (16*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup17")==0)
				msgId= (long) (17*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup18")==0)
				msgId= (long) (18*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup19")==0)
				msgId= (long) (19*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup20")==0)
				msgId= (long) (20*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup21")==0)
				msgId= (long) (21*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup22")==0)
				msgId= (long) (22*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup23")==0)
				msgId= (long) (23*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup24")==0)
				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("Anshus-MacBook-Pro.local")==0)
				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));

//			else
//					msgId=r.nextInt(10000);


		} catch (UnknownHostException e) {

			e.printStackTrace();
		}






//		msgId=r.nextInt(10000);
		_collector = collector;
		this.eventGen = new EventGen(this,this.scalingFactor);
		this.eventQueue = new LinkedBlockingQueue<List<String>>();
		String uLogfilename=this.outSpoutCSVLogFileName+msgId;
		this.eventGen.launch(this.csvFileName, uLogfilename); //Launch threads

		ba=new BatchedFileLogging(uLogfilename, context.getThisComponentId());


	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		//List<String> fieldsList = EventGen.getHeadersFromCSV(csvFileName);
		//fieldsList.add("MSGID");
		//declarer.declare(new Fields(fieldsList));
		declarer.declare(new Fields("RowString", "MSGID"));
	}

	@Override
	public void receive(List<String> event) {
		// TODO Auto-generated method stub
		//System.out.println("Called IN SPOUT### ");
		try {
			this.eventQueue.put(event);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
