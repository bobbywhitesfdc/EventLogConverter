/**
 * 
 */
package com.salesforce.labs.shield.evt;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Scanner;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.salesforce.labs.shield.evt.EventLogConverter;

/**
 * @author bobby.white
 *
 */
public class EventLogConverterTest {
	
	public static final String NEWLINE = EventLogConverter.NEWLINE;
	
	public static final String HEADER1 = "\"EVENT_TYPE\",\"TIMESTAMP\",\"REQUEST_ID\",\"ORGANIZATION_ID\",\"USER_ID\",\"USER_TYPE\",\"SESSION_TYPE\",\"SESSION_LEVEL\",\"CLIENT_IP\",\"BROWSER_TYPE\",\"PLATFORM_TYPE\",\"RESOLUTION_TYPE\",\"APP_TYPE\",\"CLIENT_VERSION\",\"API_TYPE\",\"API_VERSION\",\"USER_NAME\",\"USER_INITIATED_LOGOUT\",\"TIMESTAMP_DERIVED\",\"USER_ID_DERIVED\"";
	private static final String HEADER1RESULT = HEADER1.replace("EVENT_TYPE","eventType")
			+ EventLogConverter.SEPARATOR 
			+ EventLogConverter.NEWRELIC_TIMESTAMP_LABEL
	        + EventLogConverter.NEWLINE;
	
	public static final String DATA1 ="\"Logout\",\"20170729001030.906\",\"\",\"00D0j0000000OPd\",\"0050j000000HWLe\",\"S\",\"U\",\"1\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0\",\"2017-07-29T00:10:30.906Z\",\"0050j000000HWLeAAO\"";

	public static final String DATA2 ="\"Logout\",\"20170729001030.906\",\"\",\"00D0j0000000OPd\",\"0050j000000HWLe\",\"S\",\"F\",\"1\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0\",\"2017-07-29T00:10:30.906Z\",\"0050j000000HWLeAAO\"";
	
	public static final String FILE1 = HEADER1 + NEWLINE + DATA1 + NEWLINE + DATA2 + NEWLINE;

	public static final String TESTFILE1 = "foo.csv";
	public static final String TESTFILE2 = "bar.csv";

	private static final String TEST_TS1 = "20170729001030.906";
	private static final long TEST_TS1OUT = 1501301430906L;
	public static final String DATA1_MOD = DATA1 + ",\"" + Long.toString(TEST_TS1OUT)+ "\"";
	

	private PrintStream outputStream;

	private InputStream inputStream;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		this.inputStream = new ByteArrayInputStream(FILE1.getBytes());		
		this.outputStream = new PrintStream(new ByteArrayOutputStream());
		
		System.setIn(this.inputStream);
		System.setOut(this.outputStream);
	}
	
	@Test
	public void testProcessCLArgs1() {
		String args[] = {};
		
		EventLogConverter cvt = new EventLogConverter();
		cvt.processCommandlineArgs(args);
		
		assertEquals(null,cvt.getInputFilename());
		assertEquals(null,cvt.getOutputFilename());	
	}
	
	@Test
	public void testProcessCLArgs2() {
		String args[] = { "-i", TESTFILE1 };
		
		EventLogConverter cvt = new EventLogConverter();
		cvt.processCommandlineArgs(args);
		
		assertEquals(TESTFILE1,cvt.getInputFilename());
		assertEquals(null,cvt.getOutputFilename());	
	}
	
	@Test
	public void testProcessCLArgs3() {
		String args[] = { "-o", TESTFILE2 };
		
		EventLogConverter cvt = new EventLogConverter();
		cvt.processCommandlineArgs(args);
		
		assertEquals(null,cvt.getInputFilename());
		assertEquals(TESTFILE2,cvt.getOutputFilename());	
	}
	
	@Test
	public void testProcessCLArgs4() {
		String args[] = { "-i", TESTFILE1, "-o", TESTFILE2 };
		
		EventLogConverter cvt = new EventLogConverter();
		cvt.processCommandlineArgs(args);
		
		assertEquals(TESTFILE1,cvt.getInputFilename());
		assertEquals(TESTFILE2,cvt.getOutputFilename());	
	}

	@Test
	public void testProcessCLArgs5() {
		String args[] = { "-i" };
		
		EventLogConverter cvt = new EventLogConverter();
		
		try {
			cvt.processCommandlineArgs(args);
			fail("Expected exception for missing argument");
		} catch (IllegalArgumentException ex) {
			assert(ex.getMessage().contains(EventLogConverter.MISSING_VALUE));
		} catch (AssertionError ae) {
			throw ae; // just rethrow
		} catch (Throwable t) {
			fail("Wrong exception type!" + t.getClass().getName());
		}
	}
	
	@Test
	public void testProcessCLArgs6() {
		String args[] = { "-i","-z" };
		
		EventLogConverter cvt = new EventLogConverter();
		
		try {
			cvt.processCommandlineArgs(args);
			fail("Expected exception for missing argument");
		} catch (IllegalArgumentException ex) {
			assert(ex.getMessage().contains(EventLogConverter.EXPECTED_A_VALUE));
		} catch (AssertionError ae) {
			throw ae; // just rethrow
		} catch (Throwable t) {
			fail("Wrong exception type!" + t.getClass().getName());
		}
	}
	
	@Test
	public void testProcessCLArgsHelp() {
		String args[] = { "-h" };
		
		EventLogConverter cvt = new EventLogConverter();
		
		assert(!cvt.processCommandlineArgs(args));

	}

	/**
	 * Test method for {@link com.salesforce.labs.shield.evt.EventLogConverter#main(java.lang.String[])}.
	 */
	@Test
	public void testMain() {
		System.setIn(this.inputStream);
		System.setOut(this.outputStream);
		
		String args[] = {};
		
		EventLogConverter.main(args);
		
		
	}

	/**
	 * Test method for {@link com.salesforce.labs.shield.evt.EventLogConverter#convert(java.util.Scanner, java.io.Writer)}.
	 */
	@Test
	public void testConvert() {		
		EventLogConverter cvt = new EventLogConverter();
		try {
			cvt.convert();
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	
	@Test
	public void testConvertHeader() {
		InputStream is = new ByteArrayInputStream(HEADER1.getBytes());
		Scanner testInput = new Scanner(is);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream testOutput = new PrintStream(baos);
		
		EventLogConverter cvt = new EventLogConverter();
		
		try {
			cvt.convertHeader(testInput, testOutput);
			String content = new String(baos.toByteArray(), StandardCharsets.UTF_8);
			assertEquals(HEADER1RESULT,content);
		} catch (IOException e) {
			fail(e.getMessage());
		}
	}
	
	/**
	 * Test method for {@link com.salesforce.labs.shield.evt.EventLogConverter#convertDataLine(java.String)}.
	 */
	@Test
	public void testConvertDataLine() {
		EventLogConverter cvt = new EventLogConverter();
		
		String output = cvt.convertDataLine(DATA1);
		
		assertEquals(output,DATA1_MOD);
	}
	
	@Test
	public void testConvertTS1() {
		EventLogConverter cvt = new EventLogConverter();
		long timestamp = cvt.convertTS(TEST_TS1);
		Date asDate = new Date(timestamp);
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(new Date(timestamp));
	    assertEquals("Year not equal",2017,cal.get(Calendar.YEAR));
	    assertEquals("Month not equal",7,cal.get(Calendar.MONTH)+1);
	    assertEquals("Day not equal",29,cal.get(Calendar.DAY_OF_MONTH));
	    assertEquals("Hour not equal",0,cal.get(Calendar.HOUR));
	    assertEquals("Minute not equal",10,cal.get(Calendar.MINUTE));
	    assertEquals("Second not equal",30,cal.get(Calendar.SECOND));
		assertEquals(TEST_TS1OUT, timestamp);
	}
	
	@Test
	public void testConvertTS2() {
		EventLogConverter cvt = new EventLogConverter();
		long timestamp = cvt.convertTS("foo");
		assertEquals(0, timestamp);
	}

}
