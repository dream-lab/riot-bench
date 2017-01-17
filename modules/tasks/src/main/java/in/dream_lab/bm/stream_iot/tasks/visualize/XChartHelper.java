package in.dream_lab.bm.stream_iot.tasks.visualize;

import org.knowm.xchart.VectorGraphicsEncoder.VectorGraphicsFormat;
import org.knowm.xchart.internal.chartpart.Chart;

import de.erichseifert.vectorgraphics2d.EPSGraphics2D;
import de.erichseifert.vectorgraphics2d.PDFGraphics2D;
import de.erichseifert.vectorgraphics2d.ProcessingPipeline;
import de.erichseifert.vectorgraphics2d.SVGGraphics2D;

public class XChartHelper
{
	
	
	@SuppressWarnings("rawtypes")
	public byte[] getBytesFromVectorGraphicsChart(Chart chart, VectorGraphicsFormat format) throws Exception{
		ProcessingPipeline g = null;

	    switch (format) {
		    case EPS:
		      g = new EPSGraphics2D(0.0, 0.0, chart.getWidth(), chart.getHeight());
		      break;
		    case PDF:
		      g = new PDFGraphics2D(0.0, 0.0, chart.getWidth(), chart.getHeight());
		      break;
		    case SVG:
		      g = new SVGGraphics2D(0.0, 0.0, chart.getWidth(), chart.getHeight());
		      break;
	
		    default:
		    	throw new Exception("Unsupported vector grpahics format.");
	    }
	    chart.paint(g, chart.getWidth(), chart.getHeight());
	    return g.getBytes();
	}
}
