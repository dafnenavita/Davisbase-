package com.databaseEngine;
import java.util.HashMap;
import com.databaseEngine.Constants;

public class DataTypeDefinitions {

	public static final String TINYINT = "tinyint";
	public static final String SMALLINT = "smallint";
	public static final String INT = "int";
	public static final String BIGINT = "bigint";
	public static final String REAL = "real";
	public static final String DOUBLE = "double";
	public static final String DATETIME = "datetime";
	public static final String DATE = "date";
	public static final String TEXT = "text";

	public static HashMap<String, Byte> map1 = new HashMap<>();
	static {
		map1.put(TINYINT, (byte) 0x04);
		map1.put(SMALLINT, (byte) 0x05);
		map1.put(INT, (byte) 0x06);
		map1.put(BIGINT, (byte) 0x07);
		map1.put(REAL, (byte) 0x08);
		map1.put(DOUBLE, (byte) 0x09);
		map1.put(DATETIME, (byte) 0x0A);
		map1.put(DATE, (byte) 0x0B);
		map1.put(TEXT, (byte) 0x0C);
	}
	
	public static HashMap<Byte, String> map2 = new HashMap<>();
	static {
		map2.put( (byte) 0x04,TINYINT);
		map2.put( (byte) 0x05,SMALLINT);
		map2.put( (byte) 0x06,INT);
		map2.put( (byte) 0x07,BIGINT);
		map2.put( (byte) 0x08,REAL);
		map2.put( (byte) 0x09,DOUBLE);
		map2.put( (byte) 0x0A,DATETIME);
		map2.put( (byte) 0x0B,DATE);
		map2.put( (byte) 0x0C,TEXT);
	}

	public static HashMap<String, Integer> map3 = new HashMap<>();
	static {
		map3.put(TINYINT, 1);
		map3.put(SMALLINT, 2);
		map3.put(INT, 4);
		map3.put(BIGINT, 8);
		map3.put(REAL, 4);
		map3.put(DOUBLE, 8);
		map3.put(DATETIME, 8);
		map3.put(DATE, 8);
		map3.put(TEXT, 0);
	}

	public static HashMap<String, Object> map4 = new HashMap<>();
	static {
		map4.put(TINYINT, 0);
		map4.put(SMALLINT, 0);
		map4.put(INT, 0);
		map4.put(BIGINT, 0);
		map4.put(REAL, 0);
		map4.put(DOUBLE, 0);
		map4.put(DATETIME, 0);
		map4.put(DATE, 0);
		map4.put(TEXT, "");
	}

	public static HashMap<String, Byte> map5 = new HashMap<>();
	static {
		map5.put(TINYINT, (byte) 0x00);
		map5.put(SMALLINT, (byte) 0x01);
		map5.put(INT, (byte) 0x02);
		map5.put(BIGINT, (byte) 0x03);
		map5.put(REAL, (byte) 0x02);
		map5.put(DOUBLE, (byte) 0x08);
		map5.put(DATETIME, (byte) 0x08);
		map5.put(DATE, (byte) 0x08);
		map5.put(TEXT, (byte) 0x01);
	}
	
	public static String getByteValue(Byte code) {
		if(code >= 0x0C){
			return "TEXT";
		}
		return map2.get(code);
	}
	public static Object getNull(String dataType) {
		return map4.get(dataType.toLowerCase());
	}

	public static byte getType(String dataType) {
		return map5.get(dataType.toLowerCase());
	}

	public static int getSize(String dataType) {
		return map3.get(dataType.toLowerCase());
	}
	
	

	public static byte getDataType(String dataType) {
		return map1.get(dataType.toLowerCase());
	}
	

}
