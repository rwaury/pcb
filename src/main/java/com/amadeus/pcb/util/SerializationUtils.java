package com.amadeus.pcb.util;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.nio.charset.Charset;

public class SerializationUtils {
	
	public static final int IATA_AIRPORT_CODE_LEN = 3;
	
	public static final int IATA_AIRLINE_CODE_LEN = 2;
	
	public static final int TERMINAL_CODE_LEN = 2;
	
	public static final int ISO_3166_COUNTRY_CODE_LEN = 2;
	
	public static final String PADDING_CHAR = "_";
	
	private static final Charset ASCII_CHARSET = Charset.forName("US-ASCII");
	
	/**
	 * serializes String in ASCII without writing length field
	 * 
	 * @param out view to write to
	 * @param s string to write (ASCII only)
	 * @throws IOException 
	 */
	public static void serializeFixedLengthString(DataOutputView out, String s) throws IOException {
		byte bytes[] = s.getBytes(ASCII_CHARSET);
		out.write(bytes);
	}
	
	/**
	 * deserializes length bytes as ASCII String
	 * 
	 * @param in view to read string from
	 * @param length bytes to read
	 * @return deserialized string
	 * @throws IOException 
	 */
	public static String deserializeFixedLengthString(DataInputView in, int length) throws IOException {
		byte[] bytes = new byte[length];
		in.read(bytes);
		return new String(bytes, ASCII_CHARSET);
	}
	
	/**
	 * serializes 1 or 2 character string of ASCII characters
	 * 
	 * @param out view to write to
	 * @param s string to write (1 or 2 ASCII characters only)
	 * @throws IOException
	 */
	public static void serializeVariableLengthASCIIPair(DataOutputView out, String s) throws IOException {
		throw new RuntimeException("not yet implemented!");
	}
	
	/**
	 * deserializes 1 or 2 bytes as ASCII String
	 * 
	 * @param in view to read string from
	 * @return deserialized string
	 * @throws IOException 
	 */
	public static void deserializeVariableLengthASCIIPair(DataInputView in) throws IOException {
		throw new RuntimeException("not yet implemented!");
	}

}
