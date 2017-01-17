package in.dream_lab.bm.stream_iot.tasks;

import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

public class NoOperationTask extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { 
				doneSetup=true;
			}
		}
	}
	
	@Override
	protected Float doTaskLogic(Map map) 
	{
		return 0.0f;
	}
}

