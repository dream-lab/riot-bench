package in.dream_lab.bm.stream_iot.storm.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by anshushukla on 21/05/15.
 */


public class WordSpout extends BaseRichSpout {

    SpoutOutputCollector collector;
    Random r;
    long myRandomMsgId;
    String punct = "(\\p{Punct}|\\s)+";
//    private static Logger LOG = LoggerFactory.getLogger(RandomSentenceSpout.class);

    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {

        this.collector = collector;
        this.r = new Random();

    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        String[] sentences = new String[]{ "the cow jumped over the moon", "an apple a day keeps the doctor away",
                "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
        String sentence = sentences[r.nextInt(sentences.length)];
        //this.collector.emit(new Values(sentence));
        //long myMessage = Long.decode("0xfe00000000000000");
        myRandomMsgId++;
        if(myRandomMsgId > 5) myRandomMsgId = 1;

        for (String word : sentence.split(punct)) {
            this.collector.emit(new Values(word), myRandomMsgId);
            System.out.println("spout is working");
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void ack(Object id) {
//        LOG.info("The ACK value of in RandomSentenceSpout is " + id);
    }

    @Override
    public void fail(Object id) {
    }
}