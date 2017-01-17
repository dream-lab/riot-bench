package in.dream_lab.bm.stream_iot.tasks.visualize;

import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.knowm.xchart.XYChart;
import org.knowm.xchart.QuickChart;
import org.knowm.xchart.SwingWrapper;
import org.slf4j.Logger;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

public class XChartLinePlotTask extends AbstractTask<String,String>  
{
  
	@Override
	public void setup(Logger l_, Properties p_) {
		// TODO Auto-generated method stub
		super.setup(l_, p_);
	}

	@Override
	public Float doTaskLogic(Map map) 
	{
		try
		{
			String m = (String)map.get(AbstractTask.DEFAULT_KEY);
			String [] splitteddata = m.split(",");
		    
		    double [] xdata =  new double[splitteddata.length/2];
		    double [] ydata =  new double[splitteddata.length/2];	    
		    int x = 0, y= 0;
		    for(int j = 0; j< splitteddata.length ;j++)
		    {
		    	if(j%2 == 0)
		    		xdata[x++] = Double.parseDouble(splitteddata[j]);
				else
	        		ydata[y++] = Double.parseDouble(splitteddata[j]);
		    }    
		    XYChart chart = QuickChart.getChart("Sample Chart", "X", "Y", "y(x)", xdata, ydata);
		    
		    // creates a Swing applet to display chart
//		    new SwingWrapper(chart).displayChart();
		    Random r = new Random();
		    String filepath = "/home/shilpa/Sample_Chart_300_DPI_"+r.nextInt(100);
		    BitmapEncoder.saveBitmapWithDPI(chart,filepath , BitmapFormat.PNG, 300);
			setLastResult(filepath);
		    return 0.0f;
		}
		catch(Exception e)
		{
			
		}
		return null;
	}

}
