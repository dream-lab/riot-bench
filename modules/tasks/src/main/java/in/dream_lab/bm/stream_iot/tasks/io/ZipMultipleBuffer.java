package in.dream_lab.bm.stream_iot.tasks.io;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZipMultipleBuffer extends AbstractTask<InputStream, String> {

	private static final Object SETUP_LOCK = new Object();
	private static boolean doneSetup = false;
	private static int maxfilecount;
	private static String zipFilePath;
	private static String filenamePattern;
	private String zipFileName;
	
	private int counter;
	private long ts;
	private ZipOutputStream zipOutputStream;
	private FileOutputStream fileOutputStream;
	private String filenameExt;
	

	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if (!doneSetup) { // TODO: Move proeprty names to IO namespace
				maxfilecount = Integer.parseInt(p_.getProperty("VISUALIZE.ZIP_FILES_COUNT", "1"));
				zipFilePath = p_.getProperty("VISUALIZE.OUTPUT_ZIP_FILE_PATH");
				filenamePattern = p_.getProperty("VISUALIZE.PLOT_FILENAME_PATTERN");				
				filenameExt = ".svg"; // TODO: Add a FILE_EXT property
				ts = System.currentTimeMillis();
				zipFileName = new StringBuilder(zipFilePath).append(filenamePattern)
						.append(ts).append(".zip").toString();
				doneSetup = true;
			}
			try {
				counter = 0;
				fileOutputStream = new FileOutputStream(zipFileName);
				zipOutputStream = new ZipOutputStream(new BufferedOutputStream(fileOutputStream));
			} catch (Exception e) {
				e.printStackTrace(); // TODO: handle error using logger
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
						.append(ts).append(".zip").toString();
				this.fileOutputStream = new FileOutputStream(zipFileName);
				this.zipOutputStream = new ZipOutputStream(fileOutputStream);
				
				counter = 0;
				return 1.0f;
			}
		} catch (Exception e) {
			e.printStackTrace(); // TODO: handle error using logger
			// TODO: return -1
		}
		return 0.0f;
	}

}
