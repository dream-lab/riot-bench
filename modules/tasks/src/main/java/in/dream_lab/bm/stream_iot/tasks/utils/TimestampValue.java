package in.dream_lab.bm.stream_iot.tasks.utils;
/**
 * Helper class
 * @author shilps, simmhan
 *
 */
public class TimestampValue implements Comparable<TimestampValue>
{
	public float value;
	public long ts;
	public TimestampValue(String value_, String ts_) {
		value = Float.parseFloat(value_);
		ts = Long.parseLong(ts_);
	}
	@Override
	public int compareTo(TimestampValue tsv) 
	{
		return (int) (this.ts-tsv.ts);
	}
	
	
}




