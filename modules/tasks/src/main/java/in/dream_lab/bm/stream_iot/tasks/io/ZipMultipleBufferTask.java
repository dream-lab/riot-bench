package in.dream_lab.bm.stream_iot.tasks.io;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.visualize.XChartHelper;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;
import org.knowm.xchart.VectorGraphicsEncoder.VectorGraphicsFormat;
import org.knowm.xchart.XYSeries.XYSeriesRenderStyle;
import org.knowm.xchart.style.Styler.LegendPosition;
import org.slf4j.Logger;

public class ZipMultipleBufferTask extends AbstractTask<InputStream, String> {

	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int maxfilecount;
	private static String zipFilePath;
	private static String filenamePattern;
	private String zipFileName;

	private int counter;
	private long ts;
	private int random;
	private ZipOutputStream zipOutputStream;
	private FileOutputStream fileOutputStream;
	private String filenameExt;
	private HashMap<String, InputStream> bmMap;
	private ByteArrayInputStream inStream;
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if (!doneSetup) { // TODO: Move proeprty names to IO namespace
				maxfilecount = Integer.parseInt(p_.getProperty("IO.ZIPBUFFER.MAX_FILES_COUNT", "1"));
				zipFilePath = p_.getProperty("IO.ZIPBUFFER.OUTPUT_ZIP_FILE_PATH");
				filenamePattern = p_.getProperty("IO.ZIPBUFFER.FILENAME_PATTERN");				
				filenameExt = p_.getProperty("IO.ZIPBUFFER.FILENAME_EXT"); // TODO: Add a FILE_EXT property
				doneSetup = true;
			}
			try {
				counter = 0;
				Random ran = new Random();
				random = ran.nextInt();
				ts = System.currentTimeMillis();
				zipFileName = new StringBuilder(zipFilePath).append(filenamePattern)
						.append(ts).append(random).append(".zip").toString();
				fileOutputStream = new FileOutputStream(zipFileName);
				zipOutputStream = new ZipOutputStream(new BufferedOutputStream(fileOutputStream));
				XChartHelper  xchartHelper = new XChartHelper();
//				bmMap = new HashMap<String, InputStream>();
//				//Create a dummy plot 
//				double[] xdata = new double[] { 0, 3, 5, 7, 9 };
//				double[] ydata = new double[] { -3, 5, 9, 6, 5 };
//				
//				XYChart chart = new XYChartBuilder().width(800).height(600).title(getClass().getSimpleName())
//						.xAxisTitle("X").yAxisTitle("Y").build();
//
//				// Customize Chart
//				chart.getStyler().setLegendPosition(LegendPosition.InsideNE);
//				chart.getStyler().setAxisTitlesVisible(false);
//				chart.getStyler().setDefaultSeriesRenderStyle(XYSeriesRenderStyle.Line);
//				
//				chart.addSeries("temperature", xdata, ydata);
//			    chart.addSeries("humidity", xdata, ydata);
//			    chart.addSeries("air", xdata, ydata);
//			    chart.addSeries("light", xdata, ydata);
//			    chart.addSeries("dust", xdata, ydata);
//			    
//			    new SwingWrapper<XYChart>(chart).displayChart();
//			    
//			    //Get input stream buffer
//			    byte[] bytesArr = xchartHelper.getBytesFromVectorGraphicsChart(chart, VectorGraphicsFormat.SVG);
//				ByteArrayInputStream bytesInputStream = new ByteArrayInputStream(bytesArr);
//				
//				//add it to bm map 
//				bmMap.put(DEFAULT_KEY, bytesInputStream);
//				
				
			} catch (Exception e) {
				l.error("Exception in zipMultipleBuffer setup " +e.getMessage());
			}
		}
	}

	@Override
	protected Float doTaskLogic(Map<String, InputStream> map) {
		try {
			counter++;
			InputStream inputStream = map.get(AbstractTask.DEFAULT_KEY);
			byte[] bytes = IOUtils.toByteArray(inputStream);

			// Add the output stream to zipoutput stream
			StringBuilder fileNameWithExt = new StringBuilder(filenamePattern)
			.append(ts).append("-").append(counter).append(filenameExt);
			ZipEntry zipEntry = new ZipEntry(fileNameWithExt.toString());

			zipOutputStream.putNextEntry(zipEntry);
			zipOutputStream.write(bytes);

			if (counter == maxfilecount) {
				// Save the in memory files into a zipped file
				this.zipOutputStream.flush();
				this.zipOutputStream.close();
				setLastResult(zipFileName);

				// Init zip file name for next iteration
				this.ts = System.currentTimeMillis();
				
				this.zipFileName = new StringBuilder(zipFilePath).append(filenamePattern)
						.append(ts).append(random).append(".zip").toString();
				this.fileOutputStream = new FileOutputStream(zipFileName);
				this.zipOutputStream = new ZipOutputStream(fileOutputStream);

				counter = 0;
				return 1.0f;
			}
		} catch (Exception e) 
		{
			l.error("Exception in zipMultipleBuffer doTaskLogic " +e.getMessage());
			e.printStackTrace();
			return -1.0f;
		}
		return 0.0f;
	}

}