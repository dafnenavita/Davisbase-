package com.databaseEngine;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class TableDefinitions extends RandomAccessFile{
	String tablename;
	BPlustree tree;
	TableDefinitions fileName;
	String databaseName;
	

	public TableDefinitions(String name, String mode,String tablename) throws FileNotFoundException {
		super(name, mode);
		this.tablename = tablename;
		tree = new BPlustree(this);
		fileName = this;
	}
	
	public void setDatabasename(String name){
		databaseName = name;
	}
	
	public void addcell() throws IOException{
		if(tablename.equals("DavisTable")){
			System.out.println(tablename);
			String coltype[] = new String[2];
			String colvalue[] = new String[2];
			int cellsize = 0;
			
			coltype[0] = "TEXT";
			colvalue[0] = "DavisTable";
			cellsize += colvalue[0].length();
			coltype[1] = "TEXT";
			colvalue[1] = databaseName;
			cellsize += colvalue[1].length();
			tree.insert(1,coltype,colvalue,cellsize,0,false);
			cellsize = 0;
			
			coltype[0] = "TEXT";
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			coltype[1] = "TEXT";
			colvalue[1] = databaseName;
			cellsize += colvalue[1].length();
			tree.insert(2,coltype,colvalue,cellsize,0,false);
			
			
			
		}else if(tablename.equals("davisbase_columns")){
			Integer rowid = 1;
			String coltype[] = new String[6];
			String colvalue[] = new String[6];
			int cellsize = 0;
			Integer num2 = 1;
			
			coltype[0] = "TEXT";
			coltype[1] = "TEXT";
			coltype[2] = "TEXT";
			coltype[3] = "TINYINT";
			coltype[4] = "TEXT";
			coltype[5] = "TEXT";
			colvalue[0] = "DavisTable";
			cellsize += colvalue[0].length();
			colvalue[1] = "rowid";
			cellsize += colvalue[1].length();
			colvalue[2] = "INT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "PRI";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "DavisTable";
			cellsize += colvalue[0].length();
			colvalue[1] = "table_name";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "DavisTable";
			cellsize += colvalue[0].length();
			colvalue[1] = "Databasename";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			num2 = 1;
			rowid++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "rowid";
			cellsize += colvalue[1].length();
			colvalue[2] = "INT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "PRI";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "table_name";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "column_name";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "data_type";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "ordinal_position";
			cellsize += colvalue[1].length();
			colvalue[2] = "TINYINT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "is_nullable";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "COLUMN_KEY";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
			rowid++;
			num2++;
			cellsize = 0;
			colvalue[0] = "davisbase_columns";
			cellsize += colvalue[0].length();
			colvalue[1] = "Databasename";
			cellsize += colvalue[1].length();
			colvalue[2] = "TEXT";
			cellsize += colvalue[2].length();
			colvalue[3] = num2.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = "NO";
			cellsize += colvalue[4].length();
			colvalue[5] = "NULL";
			cellsize += colvalue[5].length();
			tree.insert(rowid,coltype,colvalue,cellsize,0,false);
			
		}else{
			
		}
	}
	
	
	public void createTable(TableDefinitions davistable,TableDefinitions daviscolm,String command) throws IOException{
		int davistblrowid =  davistable.tree.findlastrowid(0) +1;
		String coltype[] = new String[2];
		String colvalue[] = new String[2];
		int cellsize = 0;
		
		coltype[0] = "TEXT";
		colvalue[0] = tablename;
		cellsize += colvalue[0].length();
		coltype[1] = "TEXT";
		colvalue[1] = databaseName;
		cellsize += colvalue[1].length();
		davistable.tree.insert(davistblrowid,coltype,colvalue,cellsize,0,false);
		
		
		int daviscolmrowid = daviscolm.tree.findlastrowid(0) + 1;
		String[] columns = command.split(",");
		String columnName, dataType;
		String isNullable, columnKey;
		Integer ordinalPosition = 0;
		
		
		coltype = new String[7];
		colvalue = new String[7];
		coltype[0] = "TEXT";
		coltype[1] = "TEXT";
		coltype[2] = "TEXT";
		coltype[3] = "TINYINT";
		coltype[4] = "TEXT";
		coltype[5] = "TEXT";
		coltype[6] = "TEXT";
		
		for (int i = 0; i < columns.length; i++) {
			String column = columns[i];
			String[] tokens = column.trim().split(" ");
			columnName = tokens[0].trim();
			dataType = tokens[1].trim();
			column = column.toLowerCase();
			isNullable = column.contains("not null") ? "NO" : "YES";
			columnKey = column.contains("primary key") ? "PRI" : "NULL";
			ordinalPosition++;
			
			cellsize = 0;
			colvalue[0] = tablename;
			cellsize += colvalue[0].length();
			colvalue[1] = columnName;
			cellsize += colvalue[1].length();
			colvalue[2] = dataType;
			cellsize += colvalue[2].length();
			colvalue[3] = ordinalPosition.toString();
			cellsize += colvalue[3].length();
			colvalue[4] = isNullable;
			cellsize += colvalue[4].length();
			colvalue[5] = columnKey;
			cellsize += colvalue[5].length();
			colvalue[6] = databaseName;
			cellsize += colvalue[6].length();
			daviscolm.tree.insert(daviscolmrowid,coltype,colvalue,cellsize,0,false);
			daviscolmrowid++;
		}
		
	}
	
	public List<String[]> getHeaderdetails(String name) throws IOException{
		List<String[]> list = fileName.tree.findCollmnEntry(name);
		return list;
	}
	
	public List<String[]> getcolumnvalue(TableDefinitions tablefile,int ordinalnum,String pattern,String oper) throws IOException{
		//List<String[]> list = daviscolm.btree.findHeader(name);
		List<String[]> list = tablefile.tree.findColumnEntry(pattern,ordinalnum,oper);
		return list;
	}
	
	public List<String[]> findAll() throws IOException{
		List<String[]> list = fileName.tree.findAll();
		return list;
	}
	
	public void insert(int rowid,String[] coltype, String[] colvalue, int cellsize) throws IOException{
		fileName.tree.insert(rowid, coltype, colvalue, cellsize, 0, false);
	}
	
	public void delete(String pattern) throws IOException{
		fileName.tree.deleteInitialPattern(pattern);
	}
	
	public void delete(String pattern,int ordinalnum, String oper) throws IOException{
		fileName.tree.deleteLastPattern(pattern,ordinalnum,oper);
	}
	
	public void delete(String pattern1,int ordinalnum1, String oper1,String pattern2,int ordinalnum2, String oper2) throws IOException{
		fileName.tree.deletepattern(pattern1,ordinalnum1,oper1,pattern2,ordinalnum2,oper2);
	}
	
	public void update(String val, int ordnum, int[] Ordinalnum,String[] values, String[] oper1, String[] oper2) throws IOException{
		fileName.tree.update(val, ordnum, Ordinalnum,values, oper1, oper2);
	}
	
	public boolean comparetovalue(byte type,String value, String pattern, String oper) throws IOException{
		boolean ret = false;
		int Ivalue;
		Integer Ival;
		Float fval;
		Double dval;
		Long lval;
		if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.TINYINT)) {
			Ivalue = Integer.parseInt(value);
			int val = Integer.parseInt(pattern);
			if(oper.equals("<")){
				return (Ivalue < val);
			}else if(oper.equals(">")){
				return (Ivalue > val);
			}else if(oper.equals(">=")){
				return (Ivalue >= val);
			}else if(oper.equals("<=")){
				return (Ivalue <= val);
			}else if(oper.equals("=")){
				return (Ivalue == val);
			}
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.SMALLINT)) {
			Ivalue =  Integer.parseInt(value);
			Ival = Ivalue;
			int val = Integer.parseInt(pattern);
			if(oper.equals("<")){
				return (Ival < val);
			}else if(oper.equals(">")){
				return (Ival > val);
			}else if(oper.equals(">=")){
				return (Ival >= val);
			}else if(oper.equals("<=")){
				return (Ival <= val);
			}else if(oper.equals("=")){
				return (Ival == val);
			}
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.INT)) {
			Ival = Integer.parseInt(value);
			int val = Integer.parseInt(pattern);
			if(oper.equals("<")){
				return (Ival < val);
			}else if(oper.equals(">")){
				return (Ival > val);
			}else if(oper.equals(">=")){
				return (Ival >= val);
			}else if(oper.equals("<=")){
				return (Ival <= val);
			}else if(oper.equals("=")){
				return (Ival == val);
			}
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.BIGINT)) {
			lval = Long.parseLong(value);
			long val = Long.parseLong(pattern);
			if(oper.equals("<")){
				return (lval < val);
			}else if(oper.equals(">")){
				return (lval > val);
			}else if(oper.equals(">=")){
				return (lval >= val);
			}else if(oper.equals("<=")){
				return (lval <= val);
			}else if(oper.equals("=")){
				return (lval == val);
			}
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.REAL)) {
			fval = Float.parseFloat(value);
			float val = Float.parseFloat(pattern);
			if(oper.equals("<")){
				return (fval < val);
			}else if(oper.equals(">")){
				return (fval > val);
			}else if(oper.equals(">=")){
				return (fval >= val);
			}else if(oper.equals("<=")){
				return (fval <= val);
			}else if(oper.equals("=")){
				return (fval == val);
			}
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DOUBLE)) {
			dval = Double.parseDouble(value);
			float val = Float.parseFloat(pattern);
			if(oper.equals("<")){
				return (dval < val);
			}else if(oper.equals(">")){
				return (dval > val);
			}else if(oper.equals(">=")){
				return (dval >= val);
			}else if(oper.equals("<=")){
				return (dval <= val);
			}else if(oper.equals("=")){
				return (dval == val);
			}
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DATETIME)) {
			lval = Long.parseLong(value);
			long val = Long.parseLong(pattern);
			if(oper.equals("<")){
				return (lval < val);
			}else if(oper.equals(">")){
				return (lval > val);
			}else if(oper.equals(">=")){
				return (lval >= val);
			}else if(oper.equals("<=")){
				return (lval <= val);
			}else if(oper.equals("=")){
				return (lval == val);
			}
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DATE)) {
			lval = Long.parseLong(value);
			long val = Long.parseLong(pattern);
			if(oper.equals("<")){
				return (lval < val);
			}else if(oper.equals(">")){
				return (lval > val);
			}else if(oper.equals(">=")){
				return (lval >= val);
			}else if(oper.equals("<=")){
				return (lval <= val);
			}else if(oper.equals("=")){
				return (lval == val);
			}
		} else {
			if(oper.equals("=")){
				return (value.equals(pattern));
			}
		}
		return ret;
	}
	
	
	
	public String readByType(byte type,int val) throws IOException{
		int Ivalue;
		Integer Ival;
		Float fval;
		Double dval;
		Long lval;
		if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.TINYINT)) {
			Ivalue = this.readByte();
			Ival = Ivalue;
			return Ival.toString();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.SMALLINT)) {
			Ivalue =  this.readShort();
			Ival = Ivalue;
			return Ival.toString();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.INT)) {
			Ival = this.readInt();
			return Ival.toString();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.BIGINT)) {
			lval = this.readLong();
			return lval.toString();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.REAL)) {
			fval = this.readFloat();
			return fval.toString();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DOUBLE)) {
			dval = this.readDouble();
			return dval.toString();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DATETIME)) {
			lval = this.readLong();
			return lval.toString();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DATE)) {
			lval = this.readLong();
			return lval.toString();
		} else {
			int length = type - 12;
			return this.readString(length);
		}
	}
	
	
	
	
	public Object readByType(byte type) throws IOException{
		if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.TINYINT)) {
			return this.readByte();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.SMALLINT)) {
			return this.readShort();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.INT)) {
			return this.readInt();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.BIGINT)) {
			return this.readLong();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.REAL)) {
			return this.readFloat();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DOUBLE)) {
			return this.readDouble();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DATETIME)) {
			return this.readLong();
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DATE)) {
			return this.readLong();
		} else {
			int length = type - 12;
			return this.readString(length);
		}
	}

	public void writeByType(Object v) throws IOException {
		if (v instanceof Byte) {
			this.writeByte(((Byte) v).byteValue());
		} else if (v instanceof Short) {
			this.writeShort(((Short) v).shortValue());
		} else if (v instanceof Integer) {
			this.writeInt(((Integer) v).intValue());
		} else if (v instanceof Long) {
			this.writeLong(((Long) v).longValue());
		} else if (v instanceof Float) {
			this.writeFloat(((Float) v).floatValue());
		} else if (v instanceof Double) {
			this.writeDouble(((Double) v).doubleValue());
		} else {
			this.writeBytes(v.toString());
		}
	}
	
	public void writeByType(String v,Byte type) throws IOException 
	{
		if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.TINYINT)) {
			this.writeByte(Integer.parseInt(v));
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.SMALLINT)) {
			this.writeShort(Short.parseShort(v));
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.INT)) {
			this.writeInt(Integer.parseInt(v));
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.BIGINT)) {
			this.writeLong(Long.parseLong(v));
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.REAL)) {
			this.writeFloat(Float.parseFloat(v));
		} else if (type == DataTypeDefinitions.getDataType(DataTypeDefinitions.DOUBLE)) {
			this.writeDouble(Double.parseDouble(v));
		} else {
			this.writeBytes(v.toString());
		}
	}
	
	public String readString(int length) throws IOException 
	{
		byte[] b = new byte[length];
		this.read(b);
		return new String(b);
	}

}
