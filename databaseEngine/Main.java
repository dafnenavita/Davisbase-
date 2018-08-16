package com.databaseEngine;
import java.io.IOException;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;


 
class Main {

	static String prompt = "davisql> ";
	static String version = "v1.0b";
	static String copyright = "Â©2016 Dafne Navita";
	static boolean isExit = false;
	
	static long pageSize = 512; 

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	static QueryDefinitions query;
	
	/** ***********************************************************************
	 *  Main method
	 * @throws IOException 
	 */
    public static void main(String[] args) throws IOException {

		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = ""; 
		query = new QueryDefinitions();
		query.initialize();

		while(!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");


	}

	/** ***********************************************************************
	 *  Method definitions
	 */

	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
		/**
		 *  Help: Display supported commands
		 */
		public static void help() {
			System.out.println(line("*",100));
			System.out.println("SUPPORTED COMMANDS");
			System.out.println("All commands below are case insensitive");
			System.out.println();
			System.out.println("\tCREATE TABLE table_name(id INT primary key,name TEXT);            Creates a table ");
			System.out.println("\tINSERT INTO TABLE(Column_names) table_name VALUES(value1,value2); Insert rows in a table");
			System.out.println("\tSELECT * FROM table_name;                       				 	Display all records in the table.");
			System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  					Display records whose rowid is <id>.");
			System.out.println("\tDELETE FROM TABLE table_name WHERE rowid=<value>;                 Delete records from a table.");
			System.out.println("\tUPDATE table_name SET column_name WHERE rowid=<value>;            Updates existing records in a table");
			System.out.println("\tSHOW TABLES;                                                      Displays the tables");
			System.out.println("\tDROP TABLE table_name;                           					Remove table data and its schema.");
			System.out.println("\tCREATE DATABASE database_name;                                    Creates a database");
			System.out.println("\tSHOW DATABASE;												    Displays the databases");
			System.out.println("\tVERSION;                                        					Show the program version.");
			System.out.println("\tHELP;                                           				 	Show this help information");
			System.out.println("\tEXIT;                                            					Exit the program");
			System.out.println();
			System.out.println();
			System.out.println(line("*",100));
		}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}
	
	public static String getCopyright() {
		return copyright;
	}
	
	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}
		
	public static void parseUserCommand (String userCommand) throws IOException {
	
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		switch (commandTokens.get(0)) {
			case "insert":
				query.insertIntoTable(userCommand);
				//System.out.println("Record inserted into table");
			
				break;
			case "delete":
				query.delete(userCommand);
				//System.out.println("Record deleted");
				break;
			case "update":
				query.update(userCommand);
				//System.out.println("update success");
				break;
			case "select":
				query.parseQueryString(userCommand);
				break;
			case "show":
				if(commandTokens.get(1).toLowerCase().trim().equals("database")){
					query.showDatabase(userCommand);
				}else{
					query.showTables(userCommand);
				}
			    break;
			case "drop":
				if(commandTokens.get(1).toLowerCase().trim().equals("database")){
					query.dropDatabase(userCommand);
				}else{
					query.dropTable(userCommand);
				}
				break;
			case "create":
				if(commandTokens.get(1).toLowerCase().trim().equals("database")){
					query.createDatabase(userCommand);
				}else{
					query.parseCreateString(userCommand);
				}
				break;
			case "use":
				query.usedatabase(userCommand);
				break;
			case "help":
				help();
				break;
			case "version":
				displayVersion();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("I didn't understand the command: \"" + userCommand + "\"");
				break;
		}
	}
}