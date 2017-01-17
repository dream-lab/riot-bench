package in.dream_lab.bm.stream_iot.tasks.parse;

import org.slf4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * This task is thread-safe, and can be run from multiple threads. 
 * 
 * @author shukla, simmhan
 *
 */
public class XMLParse extends AbstractTask {

	private static final Object SETUP_LOCK = new Object();
	// static fields common to all threads
	private static boolean doneSetup = false;
	private static String xmlFileAsString;  // file contents as string
	
	public void setup(Logger l_, Properties p_) {
		super.setup(l_, p_);
		synchronized (SETUP_LOCK) {
			if(!doneSetup) { // Do setup only once for this task
				String xmlFilePath = p_.getProperty("PARSE.XML_FILEPATH");
				try {
					xmlFileAsString = readFileAsString(xmlFilePath, StandardCharsets.UTF_8);
				} catch (IOException e) {
					l.warn("Exception in reading xml file: " + xmlFilePath, e);
				}
				doneSetup=true;
			}
		}
	}
	
	@Override
	protected Float doTaskLogic(Map map ) {
		String m = (String)map.get(AbstractTask.DEFAULT_KEY);
		//for now code is independent of incoming message
		int tot_length = doXMLparseOp(xmlFileAsString,l);
		return Float.valueOf(tot_length);
	}

	/***
	 *
	 * @param input
	 * @param l
     * @return
     */
	public static int doXMLparseOp(String input,Logger l) {
		
		StudentRecordHandler recordHandler = new StudentRecordHandler();
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			saxParser.parse(new InputSource(new StringReader(input)), recordHandler);
		} catch (Exception e) {
			l.warn("Exception in parsing xml string: " + input, e);			
		}
		if(l.isInfoEnabled())
			l.info("XML length = {}", recordHandler.valueLength);
		
		return recordHandler.valueLength;
	}

	/***
	 *
	 * @param path
	 * @param encoding
	 * @return
	 * @throws IOException
     */
	public static String readFileAsString(String path, Charset encoding)
			throws IOException
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding).intern();
	}


	public static  final class StudentRecordHandler extends DefaultHandler {
		boolean bFirstName = false;
		boolean bLastName = false;
		boolean bNickName = false;
		boolean bMarks = false;
		public int valueLength=0;

		@Override
		public void startElement(String uri,
								 String localName, String qName, Attributes attributes)
				throws SAXException {
			if (qName.equalsIgnoreCase("student")) {
				String rollNo = attributes.getValue("rollno");
				valueLength+=rollNo.length();
			} else if (qName.equalsIgnoreCase("firstname")) {
				bFirstName = true;
				valueLength+=5;
			} else if (qName.equalsIgnoreCase("lastname")) {
				bLastName = true;
				valueLength+=6;
			} else if (qName.equalsIgnoreCase("nickname")) {
				bNickName = true;
				valueLength+=7;
			}
			else if (qName.equalsIgnoreCase("marks")) {
				bMarks = true;
				valueLength+=8;
			}
		}


		public void endElement(String uri,
							   String localName, String qName) throws SAXException {
			if (qName.equalsIgnoreCase("student")) {
				valueLength+=qName.length();
			}
		}
		@Override
		public void characters(char ch[],
							   int start, int length) throws SAXException {
			if (bFirstName) {
				valueLength += length+1;
				bFirstName = false;
			} else if (bLastName) {
				valueLength += length+2;
				bLastName = false;
			} else if (bNickName) {
				valueLength += length+3;
				bNickName = false;
			} else if (bMarks) {
				valueLength += length+4;
				bMarks = false;
			}
		}

	}


}
