package in.dream_lab.bm.stream_iot.storm.spouts;


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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class SampleSpoutWithData extends BaseRichSpout implements ISyntheticEventGen {

	private static final Logger LOG = LoggerFactory.getLogger("APP");

	SpoutOutputCollector _collector;
	EventGen eventGen;
	BlockingQueue<List<String>> eventQueue;
	String csvFileName;
	String outSpoutCSVLogFileName;
	String experiRunId;
	double scalingFactor;
	BatchedFileLogging ba;
	long msgId;

	public SampleSpoutWithData(){
//		this.csvFileName = "/home/ubuntu/sample100_sense.csv";
//		System.out.println("Inside  sample spout code");
		this.csvFileName = "/home/tarun/j2ee_workspace/eventGen-anshu/eventGen/bangalore.csv";
		this.scalingFactor = GlobalConstants.accFactor;
//		System.out.print("the output is as follows");
	}

	public SampleSpoutWithData(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor, String experiRunId){
		this.csvFileName = csvFileName;
		this.outSpoutCSVLogFileName = outSpoutCSVLogFileName;
		this.scalingFactor = scalingFactor;
		this.experiRunId = experiRunId;
	}

	public SampleSpoutWithData(String csvFileName, String outSpoutCSVLogFileName, double scalingFactor){
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
//			System.out.println("CHECK5:"+rowStringBuf.toString()+"---------"+rowString);
			values.add(rowString);
			msgId++;
			values.add(Long.toString(msgId));
			this._collector.emit(values);
			LOG.info("Count for {}", values );
			LOG.debug("Count1 for {}", values);
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
			// MSGID using  hostname for machine

			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup1")==0)
				msgId= (long) (1*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			if(InetAddress.getLocalHost().getHostName().compareTo("anshuStormSCsup2")==0)
				msgId= (long) (2*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
			else
				msgId= (long) (24*Math.pow(10,12)+(r.nextInt(1000)*Math.pow(10,9))+r.nextInt(10));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

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
