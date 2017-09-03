/**
 * 
 */
package com.salesforce.labs.shield.evt;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Locale;
import java.util.Scanner;

/**
 * EventLogConverter is intended to be used as a Unix-style pipeline processor
 * 
 * It reads in a RAW Salesforce Shield Event Log and performs various transformations to get it ready to 
 * consume.
 * 
 * @author bobby.white
 *
 */
public class EventLogConverter {

	public static final char QUOTE = '\"';
	public static final char COMMA = ',';
	public static final String EXPECTED_A_VALUE = "Expected a value, found a flag instead:";
	public static final String SEPARATOR = ",";
	public static final String EVENT_TYPE_LABEL = "\"EVENT_TYPE\"";
	public static final String NEWRELIC_TYPE_LABEL = "\"eventType\"";
	public static final String NEWLINE = "\n";
	public static final CharSequence NEWRELIC_TIMESTAMP_LABEL = "\"timestamp\"";
	public static final String INPUT_FLAG = "-i";
	public static final String OUTPUT_FLAG = "-o";
	public static final String MISSING_VALUE = "Expected a value, none found! Flag: ";
	private static final Object HELP_FLAG = "-h";
	
	private Scanner input;
	private  PrintStream output;

	/**
	 * Take Commandline args to control optional features
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		EventLogConverter converter = new EventLogConverter();
		
		try {
			if (!converter.processCommandlineArgs(args)) {
				return;
			}
		} catch (Throwable t) {
			System.err.println(t.getMessage());
			converter.displaySyntaxMessage();
			return;
		}
		
		try {
	        converter.initialize();
			converter.convert();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			converter.close();			
		}

	}

	public void close() {	
		output.flush();
		output.close();
	}

	/**
	 * @throws FileNotFoundException 
	 * 
	 **/
	private void initialize() throws FileNotFoundException {
		getOutput();
		getInput();	
	}

	private  PrintStream getOutput() throws FileNotFoundException {
		if (output == null) {
			if (getOutputFilename() != null) {
				this.output = new PrintStream(new FileOutputStream(getOutputFilename()));
				
			} else {
				// Write to Stdout
				this.output = System.out;	
			}
		}

		return output;
	}

	private Scanner getInput() throws FileNotFoundException {
		if (this.input == null) {
			if (getInputFilename() !=null) {
				this.input = new Scanner(new FileInputStream(getInputFilename()));
				
			} else {
				this.input = new Scanner(System.in);
			}
		}
		return this.input;
	}

	private String inputFilename;
	private String outputFilename;
	
	/**
	 *  syntax is  -flag {option} -flag  {option} -unaryFlag -flag {option}
	 *  	 unary flags don't require an option
	 * @param args
	 */
	public boolean processCommandlineArgs(String[] args) {
		
		if (args == null || args.length == 0) {
			return true;
		}
		
		Iterator<String> argIterator = new ArrayList<String>(Arrays.asList(args)).iterator();	
		
		while (argIterator.hasNext()) {
			String current = argIterator.next();
			
			if (isFlag(current)) {
				if (!processFlag(argIterator, current)) {
					return false;
				}
			}
			
		}
		return true;	
	}

	protected boolean processFlag(Iterator<String> argIterator, String current) {
		if (INPUT_FLAG.equals(current)) {
			this.setInputFilename(processOption(current,argIterator));
		} else if (OUTPUT_FLAG.equals(current)) {
			this.setOutputFilename(processOption(current,argIterator));
		} else if (HELP_FLAG.equals(current)) {
			displaySyntaxMessage();
			return false;
		} else {
			System.err.println("Invalid Flag:" + current);
			displaySyntaxMessage();
			return false;
		}
		return true;
	}

	/**
	 * Display the Commandline syntax and exit with an error
	 */
	private void displaySyntaxMessage() {
		System.err.println(this.getClass().getName() + " [-Flag <option> ...]");
		System.err.println("-i <inputfile>");
		System.err.println("-o <outputfile>");
	}

	private String processOption(final String flag, Iterator<String> argIterator) {
		String optionValue = argIterator.hasNext() ? argIterator.next() : null;
		
		if (isFlag(optionValue)) {
			throw new IllegalArgumentException(EXPECTED_A_VALUE + optionValue);
		} else if (optionValue == null) {
			throw new IllegalArgumentException(MISSING_VALUE + flag);
		} else {
			return optionValue;
		}
	}

	private boolean isFlag(String current) {
		return current == null ? false : current.startsWith("-");
	}

	/**
	 * Open the input and perform conversions on the 
	 * header and data lines
	 * 
	 */
	public void convert() throws IOException {
      
		// Read in the first line which is expected to be the header
		convertHeader(getInput(),getOutput());
		
		// Read in the rest of the lines
		
        while (input.hasNext()) {
            output.println(convertDataLine(input.nextLine()));
        }
	}

	public String convertDataLine(String nextLine) {
		String fields[] = nextLine.split("\",\"");
		String originalTS = fields[1]; // 2nd field always...
		long timestamp = convertTS(originalTS);
		
		return nextLine + COMMA +  QUOTE + Long.toString(timestamp) + QUOTE;
	}

	/**
	 * Original TS is expected to be in the form "20170729001030.906"
	 * @param originalTS  yyyyMMddhhmmss.S
	 * @return a Java long timestamp
	 */
	public long convertTS(String originalTS) {
		DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss.S", Locale.ENGLISH);
		try {
			Date date = format.parse(originalTS);
			return date.getTime();
		} catch (ParseException e) {
			return 0;
		}
	}

	public void convertHeader(Scanner input, PrintStream output) throws IOException {
		if (input.hasNext()) {
			boolean isFirst = true;
			for (String currentColumn : input.nextLine().split(SEPARATOR) ) {
				if (currentColumn.equalsIgnoreCase(EVENT_TYPE_LABEL)) {
					currentColumn = NEWRELIC_TYPE_LABEL;
				}
				
				if (isFirst) {
					isFirst = false;
				} else {
					output.append(SEPARATOR);
				}
				output.append(currentColumn);	
			}
			output.append(SEPARATOR);
			output.append(NEWRELIC_TIMESTAMP_LABEL);
			output.append(NEWLINE);
		}
		
	}

	/**
	 * @return the inputFilename
	 */
	public String getInputFilename() {
		return inputFilename;
	}

	/**
	 * @param inputFilename the inputFilename to set
	 */
	public void setInputFilename(String inputFilename) {
		this.inputFilename = inputFilename;
	}

	/**
	 * @return the outputFilename
	 */
	public String getOutputFilename() {
		return outputFilename;
	}

	/**
	 * @param outputFilename the outputFilename to set
	 */
	public void setOutputFilename(String outputFilename) {
		this.outputFilename = outputFilename;
	}

}
