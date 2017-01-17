package in.dream_lab.bm.stream_iot.tasks.visualize;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.knowm.xchart.VectorGraphicsEncoder.VectorGraphicsFormat;
import org.knowm.xchart.internal.chartpart.Chart;

import de.erichseifert.vectorgraphics2d.ProcessingPipeline;
import de.erichseifert.vectorgraphics2d.SVGGraphics2D;

public class ZipChartVectorGraphics 
{
private String output_zip_file_path;
private String zip_file_name_pattern;
private String current_zip_filename; 

	private ZipOutputStream zipOutputStream;
	
	public ZipChartVectorGraphics(String output_zip_file_path,  String zip_file_name_pattern) throws FileNotFoundException, IOException{
		this.output_zip_file_path  = output_zip_file_path;
		this.zip_file_name_pattern = zip_file_name_pattern;
		current_zip_filename = this.output_zip_file_path+ zip_file_name_pattern  +System.currentTimeMillis()+".zip";
		FileOutputStream fileOutputStream = new FileOutputStream(current_zip_filename);		
		this.zipOutputStream = new ZipOutputStream(new BufferedOutputStream(fileOutputStream));
	}
	
	public void addChartFileToZip(Chart chart, String fileName,  VectorGraphicsFormat vectorGraphicsFormat) throws IOException {
		
		/*
		 *  Create a buffered image from chart and generate its file name with extension.
		 *  Reusing the VectorGraphics functions for achieving the same.
		 */
		ProcessingPipeline g = null;
		g = new SVGGraphics2D(0.0, 0.0, chart.getWidth(), chart.getHeight());
		
		chart.paint(g, chart.getWidth(), chart.getHeight());
		
		byte[] bytesArray = g.getBytes();
		
		String fileNameWithExt = fileName + "." + vectorGraphicsFormat.toString().toLowerCase();
		
		// Add a generate image to zip and mapping it to generated fileNameWithExt 
		ZipEntry zipEntry = new ZipEntry(fileNameWithExt);
		this.zipOutputStream.putNextEntry(zipEntry);
		this.zipOutputStream.write(bytesArray);		
		
	}	
	
	public String saveZipFile() throws IOException{
		this.zipOutputStream.flush();
		this.zipOutputStream.close();
		String last_created_filename = this.current_zip_filename;
		resetZipOutputStream();
		return last_created_filename;
	}
	
	public void resetZipOutputStream() throws FileNotFoundException
	{
		this.current_zip_filename = this.output_zip_file_path+ this.zip_file_name_pattern  +System.currentTimeMillis()+".zip";
		FileOutputStream fileOutputStream = new FileOutputStream(this.current_zip_filename);
		this.zipOutputStream = new ZipOutputStream(new BufferedOutputStream(fileOutputStream));
	}
}
