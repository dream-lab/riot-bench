package in.dream_lab.bm.stream_iot.tasks.visualize;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.knowm.xchart.VectorGraphicsEncoder.VectorGraphicsFormat;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle;
import org.knowm.xchart.style.Styler.LegendPosition;
import org.slf4j.Logger;

import weka.filters.unsupervised.attribute.TimeSeriesDelta;
import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.utils.TimestampValue;

//YS: The output from this has to an "input stream" so that the receiver can read the input!!
public class XChartMultiLinePlotTask extends AbstractTask<Queue<TimestampValue>, InputStream> {  
	// static fields common to all threads
	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private XChartHelper xchartHelper;
	private Map<String, Queue<TimestampValue>> mapbm;
	private TimestampValue ts ;
	private PriorityQueue<TimestampValue> q;
	
 	@Override
	public void setup(Logger l_, Properties p_) {
		try {
			super.setup(l_, p_);
			synchronized (SETUP_LOCK) { // ONLY for static fields
				if (!doneSetup) {
					doneSetup = true;
				}
				xchartHelper = new XChartHelper();
			}
		} catch (Exception e) {
			l.error("Exception occured in XChartMultiLinePlot setup method :" +e.getMessage()); // TODO: Handle using logger!!!
		}
	}

	@Override
	public Float doTaskLogic(Map<String, Queue<TimestampValue>> map) {
		try {
			String obsType = null;
			Set<Entry<String, Queue<TimestampValue>>> entrySet =  map.entrySet();
			double[] xdata = null;
			double[] ydata = null;
			
			// Create chart
			XYChart chart = new XYChartBuilder().width(800).height(600).title(getClass().getSimpleName())
					.xAxisTitle("X").yAxisTitle("Y").build();

			// Customize Chart
			chart.getStyler().setLegendPosition(LegendPosition.InsideNE);
			chart.getStyler().setAxisTitlesVisible(false);
			chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line);
			Queue<TimestampValue> queue;
			for (Entry<String, Queue<TimestampValue>> entry : entrySet) {
				obsType = entry.getKey();
				queue = entry.getValue();
				xdata = new double[queue.size()];
				ydata = new double[queue.size()];
				TimestampValue tsValObj = null;

				long initTime = queue.peek().ts;
				int j = 0;
				while((tsValObj = queue.poll()) != null){
					xdata[j] = tsValObj.ts - initTime;
					ydata[j] = tsValObj.value;
					j++;
				}
				chart.addSeries(obsType, xdata, ydata);
			}
			byte[] bytesArr = xchartHelper.getBytesFromVectorGraphicsChart(chart, VectorGraphicsFormat.SVG);
			ByteArrayInputStream bytesInputStream = new ByteArrayInputStream(bytesArr);
			setLastResult(bytesInputStream);
		} catch (Exception e) {
			l.error("Exception occured in XChartMultiLinePlot do task method :" +e.getMessage());
			e.printStackTrace(); // TODO: Handle errors using logger!!!
			return -1.0f; // TODO: if you have an error, you return -1;
		}
		return 1.0f; // TODO: You return NULL if there is no output. Here, you do have an output. 
	}
	
}