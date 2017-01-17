package in.dream_lab.bm.stream_iot.storm.genevents;

import java.util.List;

public interface ISyntheticEventGen {
	public void receive(List<String> event);  //event
}
