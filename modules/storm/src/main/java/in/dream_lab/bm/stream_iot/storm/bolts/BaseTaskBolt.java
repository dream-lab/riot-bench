package in.dream_lab.bm.stream_iot.storm.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.ITask;
import in.dream_lab.bm.stream_iot.tasks.utils.TimestampValue;

public abstract class BaseTaskBolt extends BaseRichBolt {

	protected static Logger l;
	protected ITask task;
	protected OutputCollector collector;
	protected Properties p;

    public BaseTaskBolt(Properties p_){
         p = p_;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        l = LoggerFactory.getLogger("APP");
        task = getTaskInstance();
        task.setup(l, p);
    }

    abstract protected ITask getTaskInstance();

    @Override
    public void execute(Tuple input) {
        String rowString = input.getStringByField("RowString");
        String msgId = input.getStringByField("MSGID");
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(AbstractTask.DEFAULT_KEY, rowString);
        Float res = task.doTask(map);

        if(res != null ) {
            if(res != Float.MIN_VALUE) collector.emit(new Values(rowString, msgId, res));
            else {
                if (l.isWarnEnabled()) l.warn("Error in task {} for input {}", task, rowString);
                throw new RuntimeException("Error in task " + task + " for input " + rowString);
            }
        }
    }

    @Override
    public void cleanup() {
        task.tearDown();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("RowString","MSGID","Res"));
    }


}
