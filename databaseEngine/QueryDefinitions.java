package com.databaseEngine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class QueryDefinitions {
	static TableDefinitions davistable;
	static TableDefinitions davistable2;
	static String path_name;
	static String dataBaseName;
	
	public void setfoldername(String name){
		dataBaseName = name;
		path_name = "data/"+name+"/";
	}
	
	
	/**
	 * @throws IOException
	 */
	public void initialize() throws IOException{

		try {
			File fileName = new File("data/catalog");
			if(fileName != null){
				boolean success = fileName.mkdirs();
				if(!success){
					//System.out.println("Error");
				}
			}
			fileName = new File("data/defaultdb");
			if(fileName != null){
				boolean success = fileName.mkdirs();
				if(!success){
					//System.out.println("Error");
				}
				
			}
			setfoldername("user_data");
			davistable = new TableDefinitions("data/catalog/davisbase_tables.tbl","rw","davisbase_tables");
			if(davistable.length() == 0){
				davistable.setDatabasename("catalog");
				davistable.addcell();
			}
			davistable.setDatabasename("catalog");
			
			davistable2 = new TableDefinitions("data/catalog/davisbase_column.tbl","rw","davisbase_columns");
			if(davistable2.length() == 0){
				davistable2.setDatabasename("catalog");
				davistable2.addcell();
			}
			davistable2.setDatabasename("catalog");
			
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
	}
	

	
	// create database
	public void createDatabase(String usercommand) throws IOException{
		String[] command = usercommand.split(" ");
		File dir = new File("data/"+command[2].toLowerCase().trim());
		if(dir.exists()){
			System.out.println("Error:Database is already present");
			return;
		}else{
			dir.mkdir();
		}
	}
	
	// use database
	public void usedatabase(String usercommand) throws IOException{
		String[] command = usercommand.split(" ");
		setfoldername(command[2].toLowerCase().trim());
	}
	
	//show database
    public void showDatabase(String usercommand) throws IOException{
		List<String[]> list = davistable.getcolumnvalue(davistable2,2,"davisbase_tables","=");		
		List<String[]> list2 = davistable.findAll();
		System.out.println(list.get(2)[2]);
		HashMap<String,Integer> map = new HashMap<>();
		for(int i = 0; i < list2.size(); i++){
			String[] cell = list2.get(i);
			map.put(cell[2], 1);
		}
		
		
		File filePath = new File("data/");
		if(filePath.exists()){
			for(String f:filePath.list()){
				System.out.println(f);
			}
		}
	} 
	
	//drop database
	public void dropDatabase(String usercommand) throws IOException{
		String[] command = usercommand.split(" ");
		File dir = new File("data/"+command[2].toLowerCase().trim());
		if(dir.exists()){
			for(File f:dir.listFiles()){
				String path = f.getAbsolutePath();
				String db = path.split("/")[path.split("/").length-2];
				davistable2.delete(db, 8, "=");
				f.delete();
			}
			dir.delete();
		}
		setfoldername("user_data");
	}
	
	
	
    //show table
	/**
	 * @param usercommand
	 * @throws IOException
	 */
	public void showTables(String usercommand) throws IOException{
		List<String[]> list = davistable.getcolumnvalue(davistable2,2,"davisbase_tables","=");		
		List<String[]> list2 = davistable.findAll();
		System.out.print(list.get(1)[2]);
		System.out.print("\t"+list.get(2)[2]);
		System.out.print("\n");
		for(int i = 0; i < list2.size(); i++){
			String[] cell = list2.get(i);
			System.out.print(cell[1]);
			System.out.print("\t"+cell[2]);
			System.out.println("\n");
		}
	}
	
	
	//Insert into table
	/**
	 * @param command
	 * @throws IOException
	 */
	public void insertIntoTable(String command) throws IOException{
		String tableName;
		String[] data;
		String[] col2 = {};
		if(command.indexOf("(") == command.lastIndexOf("(")){
			String str1 = command.substring(0,command.toLowerCase().indexOf("values"));
			String str2 = command.substring(command.toLowerCase().indexOf("values")+7,command.length());
			
			String[] str1arr = str1.trim().split(" ");
			tableName = str1arr[2].toLowerCase().trim();
			
			str2 = str2.replaceAll("[()']", "");
			data = str2.split(",");
		}else{
			tableName = command.substring(command.indexOf(")")+1,command.toLowerCase().indexOf("values")-1).trim();
			data = command.substring(command.lastIndexOf("(") + 1, command.lastIndexOf(")")).split(",");
			col2 = command.substring(command.indexOf("(") + 1, command.indexOf(")")).split(",");
		}
		

		List<String[]> initial = davistable2.getHeaderdetails(tableName);
		List<String[]> update = new ArrayList<>();
		
		for(int i = 0; i < initial.size(); i++){
			String[] cell = initial.get(i);
			if(cell[7].equals(dataBaseName)){
				update.add(cell);
			}
		}
		
		TableDefinitions tablefile = new TableDefinitions(path_name+tableName+".tbl","rw",tableName);
	
		if(update.size() == 0){
			System.out.println("Error : The Table you are trying to insert is not available");
			tablefile.close();
			return;
		}
		
		int size = 0;
		int row = 0;
		String[] a = new String[data.length-1];
		String[] b= new String[data.length-1]; 
	
		
		for(int i = 0; i < update.size(); i++){
			String[] col1 = update.get(i);
			if(col2.length != 0){
				if(!col1[2].equals(col2[i].trim())){
					System.out.println("Error :The columns entered are not in accordance with the columns of the table");
					tablefile.close();
					return;
				}
			}
			if(col1[5].toLowerCase().equals("no") && data[i].trim().toLowerCase().equals("null")){
				System.out.println("Error : Value for "+col1[2]+" cannot be NULL");
				tablefile.close();
				return;
			}
			
			if(i == 0){
				if(data[i].contains("[^0-9]")){
					System.out.println("Error :Primary key constraint");
				}else{
					row = Integer.parseInt(data[i].trim());
				}
			}else{
				if(col1[3].equals("float")){
					col1[3] = "double";
				}
				a[i-1] = col1[3];
				b[i-1] = data[i].trim();
				if(DataTypeDefinitions.getSize(col1[3]) != 0){
					size += DataTypeDefinitions.getSize(col1[3]);
				}else{
					size += data[i].trim().length();
				}
			}
		}
		
		tablefile.insert(row,a,b,size);
		tablefile.close();
	}
	
	// Update Table
	/**
	 * @param command
	 * @throws IOException
	 */
	public void update(String command) throws IOException{
		String str1 = command.substring(0, command.toLowerCase().indexOf("where"));
		String str2 = command.substring(command.toLowerCase().indexOf("where")+6, command.length());
		
		String[] str1arr = str1.split(" ");
		String[] str2arr = str2.split(" ");
		
		String tableName = str1arr[1];
		String colm = str1arr[3];
		String val = str1arr[5];
		int num4 = 0;
		
		String[] col4 = new String[str2arr.length/3];
		String[] oper1 = new String[str2arr.length/3];
		String[] values = new String[str2arr.length/3];
		String[] oper2 = new String[(str2arr.length/3)-1];
		int[] num1 = new int[str2arr.length/3];
		int j = 0;
		for(int i = 0; i < str2arr.length; i+=4){
			col4[j] = str2arr[i];
			oper1[j] = str2arr[i+1];
			values[j] = str2arr[i+2];
			if((i+3) < str2arr.length){
				oper2[j] = str2arr[i+3];
			}
			j++;
		}
		
		String tableFileName = path_name+tableName+".tbl";
		TableDefinitions file = new TableDefinitions(tableFileName,"rw",tableName);
		
		List<String[]> templist = davistable2.getHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();
		
		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(dataBaseName)){
				list.add(cell);
			}
		}
		
		int num7 = 0;
		for(int i = 0; i < list.size(); i++){
			String[] cell = list.get(i);
			String col2 = cell[2];
			if(colm.equals(col2)){
				num4 = Integer.parseInt(cell[4]);
			}
			for(int k=0; k < col4.length; k++){
				if(col2.equals(col4[k])){
					num1[k] = Integer.parseInt(cell[4]);
					num7++;
				}
			}
		}
		if((num7 != (str2arr.length/3)) || num4 == 0){
			System.out.println("Error : Invalid Column name");
			file.close();
			return;
		}
		
		file.update(val, num4, num1,values, oper1, oper2);
		file.close();
	}
	
	/**
	 *  Stub method for dropping tables
	 *  @param dropTableString is a String of the user input
	 * @throws IOException 
	 */
	public void dropTable(String dropTableString) throws IOException {

		String tablename = dropTableString.toLowerCase().split(" ")[2];
		davistable.delete(tablename,2,"=",dataBaseName,3,"=");
		davistable2.delete(tablename,2,"=",dataBaseName,8,"=");
		File delfile = new File(path_name+tablename+".tbl");
		if(delfile.exists()){
			delfile.delete();
		}
		
	}
	
	//Delete Table
	/**
	 * @param command
	 * @throws IOException
	 */
	public void delete(String command) throws IOException {
		String[] userInput = command.split(" ");
		if(userInput.length < 8){
			System.out.println("Error : invalid delete command format ");
			return;
		}
		
		String tableName = userInput[3];
		String colmName = userInput[5];
		String oper = userInput[6];
		String stringPattern = userInput[7];
		String tableFileName = path_name+tableName+".tbl";
		TableDefinitions file = new TableDefinitions(tableFileName,"rw",tableName);
		int num9 = 0;
		List<String[]> templist = davistable2.getHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();
		
		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(dataBaseName)){
				list.add(cell);
			}
		}
		for(int i = 0; i < list.size(); i++){
			String[] cell = list.get(i);
			if(colmName.equals(cell[2])){
				num9= Integer.parseInt(cell[4]);
			}
		}
		if(num9 == 0){
			System.out.println("Error:Invalid column name");
			file.close();
			return;
		}
		file.delete(stringPattern, num9, oper);
		
		file.close();
 	}
	
	/**
	 *  Stub method for executing queries
	 *  @param queryString is a String of the user input
	 * @throws IOException 
	 */
	public void parseQueryString(String command) throws IOException {
		String[] commandstr = command.split(" ");
		String dispcolm = commandstr[1].toLowerCase().trim();
		String tableName = commandstr[3].toLowerCase().trim();
		String tableFileName = path_name+tableName+".tbl";
		TableDefinitions file = new TableDefinitions(tableFileName,"rw",tableName);
		List<String[]> templist = davistable2.getHeaderdetails(tableName);
		List<String[]> list = new ArrayList<>();
		
		for(int i = 0; i < templist.size(); i++){
			String[] cell = templist.get(i);
			if(cell[7].equals(dataBaseName)){
				list.add(cell);
			}
		}
		int ordinalnum = 0;
		List<String[]> list2;
		if(commandstr.length > 4){
			String colmname = commandstr[5].toLowerCase().trim();
			String operator = commandstr[6].toLowerCase().trim();
			String value = commandstr[7].toLowerCase().trim();
			for(int i = 0; i < list.size(); i++){
				String[] cell = list.get(i);
				if(colmname.equals(cell[2])){
					ordinalnum= Integer.parseInt(cell[4]);
				}
			}
			if(ordinalnum == 0){
				System.out.println("column name given is not valid column name");
				file.close();
				return;
			}
			list2 = file.getcolumnvalue(file,ordinalnum,value,operator);
			
		}else{
			list2 = file.findAll();
		}
		
		if(dispcolm.equals("*")){
			for(int i = 0; i < list.size(); i++){
				String[] cell = list.get(i);
				System.out.print(""+cell[2]+"\t");
			}
			System.out.print("\n");
			
			for(int i = 0; i < list2.size(); i++){
				String[] cell = list2.get(i);
				for(int j = 0; j < cell.length; j++){
					System.out.print(""+cell[j]+"\t");
				}
				System.out.print("\n");
				
			}
		}	
		file.close();
		
	}
	
	/**
	 *  Stub method for creating new tables
	 *  @param command is a String of the user input
	 */
	public void parseCreateString(String command) {


		String createTable = "create table";
		String tableName = command.substring(createTable.length(), command.indexOf("(")).trim();
		command = command.substring(command.indexOf("(") + 1, command.lastIndexOf(")"));
		
		/*  Code to create a .tbl file to contain table data */
		try {
			/*  Create RandomAccessFile tableFile in read-write mode.
			 * 
			 */
			
			List<String[]> templist = davistable2.getHeaderdetails(tableName);
			List<String[]> list = new ArrayList<>();
			
			for(int i = 0; i < templist.size(); i++){
				String[] cell = templist.get(i);
				if(cell[7].equals(dataBaseName)){
					list.add(cell);
				}
			}
			
			if(list.size() > 0){
				System.out.println("Table already exists");
				return;
			}
			String tableFileName = path_name+tableName+".tbl";
			TableDefinitions file = new TableDefinitions(tableFileName,"rw",tableName);
			
			
			file.setDatabasename(dataBaseName);
			file.createTable(davistable,davistable2,command);
			file.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
		
	
	}

}
