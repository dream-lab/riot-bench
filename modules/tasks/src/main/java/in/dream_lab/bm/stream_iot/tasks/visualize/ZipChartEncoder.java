package in.dream_lab.bm.stream_iot.tasks.visualize;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.imageio.ImageIO;

import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.BitmapEncoder.BitmapFormat;
import org.knowm.xchart.internal.chartpart.Chart;

public class ZipChartEncoder {
	
	private String output_zip_file_path;
	
	private ZipOutputStream zipOutputStream;
	
	public ZipChartEncoder(String output_zip_file_path) throws FileNotFoundException, IOException{
		this.output_zip_file_path  = output_zip_file_path;
		FileOutputStream fileOutputStream = new FileOutputStream(this.output_zip_file_path);
		this.zipOutputStream = new ZipOutputStream(new BufferedOutputStream(fileOutputStream));
	}
	
	@SuppressWarnings("rawtypes")
	public void addChartFileToZip(Chart chart, String fileName, BitmapFormat bitmapFormat) throws IOException {
		
		/*
		 *  Create a buffered image from chart and generate its file name with extension.
		 *  Reusing the Bitmap Encoder functions for achieving the same.
		 */
		BufferedImage image = BitmapEncoder.getBufferedImage(chart);
		String fileNameWithExt = BitmapEncoder.addFileExtension(fileName, bitmapFormat);
		
		// Add a generate image to zip and mapping it to generated fileNameWithExt 
		ZipEntry zipEntry = new ZipEntry(fileNameWithExt);
		this.zipOutputStream.putNextEntry(zipEntry);
		ImageIO.write(image, bitmapFormat.toString().toLowerCase(), this.zipOutputStream);
	}	
	
	
	public void saveZipFile() throws IOException{
		this.zipOutputStream.flush();
		this.zipOutputStream.close();
	}
}
