package in.dream_lab.bm.stream_iot.tasks.io;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.tuple.Values;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;

public class MQTTSubscribeTask extends AbstractTask implements MqttCallback
{
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int useMsgField;
	private static String apolloUserName;
	private static String apolloPassword;
	private static String apolloURLlist;

	private static String topic;


	/* local fields assigned to each thread */
	private MqttClient mqttClient;
	private String apolloClient;
	private String   apolloURL;

	public LinkedBlockingQueue<String> incoming=new LinkedBlockingQueue<>(); // added later

	public void setup(Logger l_, Properties p_) 
	{
		super.setup(l_, p_);
		try
		{
			synchronized (SETUP_LOCK) 
			{ 
				if (!doneSetup) 
				{ 
//					useMsgField = Integer.parseInt(p_.getProperty("IO.MQTT_SUBSCRIBE.USE_MSG_FIELD"));
					apolloUserName = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_USER");
					apolloPassword = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_PASSWORD");
					apolloURL = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_URL");
					topic = p_.getProperty("IO.MQTT_SUBSCRIBE.TOPIC_NAME");
					doneSetup = true;
				}
				apolloClient = p_.getProperty("IO.MQTT_SUBSCRIBE.APOLLO_CLIENT");
				mqttClient = new MqttClient(apolloURL, apolloClient);
	            MqttConnectOptions connOpts = new MqttConnectOptions();
	            connOpts.setCleanSession(true);
	            connOpts.setUserName(apolloUserName);
	            connOpts.setPassword(apolloPassword.toCharArray());
	            mqttClient.connect(connOpts);
	            mqttClient.setCallback(this);
				mqttClient.subscribe(topic); // added later
//				this.incoming = new LinkedBlockingQueue<>(); // added later
			}
		}
			catch(Exception e )
			{
				e.printStackTrace();
			}
		}


		@Override
		protected Float doTaskLogic(Map map) 
		{
			String m = (String)map.get(AbstractTask.DEFAULT_KEY);

			String pollString = incoming.poll();
			if(pollString!=null) {
				if(l.isInfoEnabled())
				l.info("TEST:pollString-{}",pollString);
				setLastResult(pollString);
				return Float.valueOf(pollString.length());
			}

			if(pollString==null){
				setLastResult(null);
			}

		return null;

		}

		@Override
		public void connectionLost(Throwable arg0)
		{
			
		}

		@Override
		public void deliveryComplete(IMqttDeliveryToken arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void messageArrived(String arg0, MqttMessage arg1)
				throws Exception 
		{
			if(l.isInfoEnabled())
			l.info("In messageArrived {}",arg1.toString());

			if(arg1!=null)
			incoming.put(arg1.toString());
//			setLastResult(arg1.toString());
		}
}