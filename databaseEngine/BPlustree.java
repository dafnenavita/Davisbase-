package com.databaseEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BPlustree {

	public TableDefinitions table;
	static int pagesize = 512 ;
	
	public BPlustree(TableDefinitions table){
		this.table = table;
	}
	
	/**
	 * @param pageno
	 * @return
	 * @throws IOException
	 */
	public boolean intPosition(int pageno) throws IOException{
		int pos = (pageno) * pagesize;
		pos += 1;
		
		table.seek(pos);
		Byte byteCode = table.readByte();
		if(byteCode < ((pagesize-8)/10)){
			return true;
		}else{
			return false;
		}
	}

	/**
	 * @param pageno
	 * @param leafpageno
	 * @param rowid
	 * @throws IOException
	 */
	public void addintialCellValue(int pageno,Integer leafpageno,Integer rowid) throws IOException{
		int pos = pageno * pagesize;
		table.seek(pos);
		
			String[] colvalue = new String[2];
			colvalue[1] = rowid.toString();
			colvalue[0] = leafpageno.toString();
			PageDefinitions page = new PageDefinitions(table, pageno,colvalue, 0x05);
			page.writeinterior(rowid,leafpageno);
		
	}
	
	/**
	 * @param pattern
	 * @throws IOException
	 */
	public void deleteInitialPattern(String pattern) throws IOException{
		int position = findInitialLeaf() * pagesize;
		int initialPosition = 0;
		int columnNo = 0;
		int pointerPosition = table.readInt();
		int patternLast = 0;
		do{
			table.seek(position+1);
			int colnum = table.readByte();
			table.seek(position+4);
			pointerPosition = table.readInt();
			int finialPosition = position +8;
			initialPosition = 0;
			for(int i = 0; i < colnum; i++){
				patternLast = 0;
				table.seek(finialPosition+(2*i));
				initialPosition = table.readShort();
				table.seek(position+initialPosition+6);
				columnNo = table.readByte();
				String columnValue;
				int columnType = position+initialPosition+7;
				int columnPosit = position+initialPosition+7+columnNo;
				for(int j = 0; j < columnNo; j++){
					columnType = position+initialPosition+7+j;
					table.seek(columnType);
					Byte byteType = table.readByte();
					table.seek(columnPosit);
					columnValue = table.readByType(byteType,1);
					if(columnValue.equals(pattern)){
						patternLast = 1;
					}
					int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(byteType));
					if(size == 0){
						columnPosit += columnValue.length();
					}else{
						columnPosit += size;
					}
				}
				if(patternLast == 1){
					table.seek(position+initialPosition);
					int cellsize = table.readShort()+6;
					for(int k = 0; k < cellsize; k++){
						table.seek(position+initialPosition+k);
						table.writeByte(0x00);
					}
					table.seek(finialPosition+(2*i));
					table.writeShort(0);
					if(i == colnum-1){
						table.seek(position+1);
						table.writeByte(colnum-1);
					}
				}
			}
			position = (pointerPosition == -1) ? position :pointerPosition*pagesize;
		}while(pointerPosition != -1);
	}
	
	
	/**
	 * @param pattern
	 * @param cardNum
	 * @param funcOper
	 * @throws IOException
	 */
	public void deleteLastPattern(String pattern, int cardNum, String funcOper) throws IOException{
		int position = findInitialLeaf() * pagesize;
		int positionValue = 0;
		int columnNumber = 0;
		int rightptr = table.readInt();
		int patternInitial = 0;
		int pointerValue = 0;
		do{
			table.seek(position+1);
			int num_cells = table.readByte();
			table.seek(position+4);
			rightptr = table.readInt();
			int newpos = position +8;
			positionValue = 0;
			for(int i = 0; i < num_cells; i++){
				patternInitial = 0;
				pointerValue = 1;
				table.seek(newpos+(2*i));
				positionValue = table.readShort();
				if(positionValue == 0){
					continue;
				}
				table.seek(position+positionValue+6);
				columnNumber = table.readByte();
				table.seek(position+positionValue+2);
				Integer rowid = table.readInt();
				String colvalue = rowid.toString();
				if(pointerValue == cardNum){
					if(table.comparetovalue((byte)0x06, colvalue, pattern, funcOper)){
						patternInitial = 1;
					}
				}
				int coltypepos = position+positionValue+7;
				int colvalpos = position+positionValue+7+columnNumber;
				for(int j = 0; j < columnNumber; j++){
					pointerValue++;
					coltypepos = position+positionValue+7+j;
					table.seek(coltypepos);
					Byte type = table.readByte();
					table.seek(colvalpos);
					colvalue = table.readByType(type,1);
					if(pointerValue == cardNum){
						if(table.comparetovalue(type, colvalue, pattern, funcOper)){
							patternInitial = 1;
						}
					}
					int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
					if(size == 0){
						colvalpos += colvalue.length();
					}else{
						colvalpos += size;
					}
				}
				if(patternInitial == 1){
					table.seek(position+positionValue);
					int cellsize = table.readShort()+6;
					for(int k = 0; k < cellsize; k++){
						table.seek(position+positionValue+k);
						table.writeByte(0x00);
					}
					table.seek(newpos+(2*i));
					table.writeShort(0);
					if(i == num_cells-1){
						table.seek(position+1);
						table.writeByte(num_cells-1);
					}
				}
			}
			position = (rightptr == -1) ? position :rightptr*pagesize;
		}while(rightptr != -1);
	}
	
	/**
	 * @param pattern
	 * @param num3
	 * @param oper
	 * @param pattern2
	 * @param num6
	 * @param oper2
	 * @throws IOException
	 */
	public void deletepattern(String pattern, int num3, String oper,String pattern2,int num6, String oper2) throws IOException{
		int pos = findInitialLeaf() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = table.readInt();
		int patternfound = 0;
		int pattern2found = 0;
		int num5 = 0;
		do{
			table.seek(pos+1);
			int num_cells = table.readByte();
			table.seek(pos+4);
			rightptr = table.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				pattern2found = 0;
				num5 = 1;
				table.seek(newpos+(2*i));
				cellpos = table.readShort();
				if(cellpos == 0){
					continue;
				}
				table.seek(pos+cellpos+6);
				num_colms = table.readByte();
				table.seek(pos+cellpos+2);
				Integer rowid = table.readInt();
				String colvalue = rowid.toString();
				if(num5 == num3){
					if(table.comparetovalue((byte)0x06, colvalue, pattern, oper)){
						patternfound = 1;
					}
				}
				if(num5 == num6){
					if(table.comparetovalue((byte)0x06, colvalue, pattern2, oper2)){
						pattern2found = 1;
					}
				}
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					num5++;
					coltypepos = pos+cellpos+7+j;
					table.seek(coltypepos);
					Byte type = table.readByte();
					table.seek(colvalpos);
					colvalue = table.readByType(type,1);
					if(num5 == num3){
						if(table.comparetovalue(type, colvalue, pattern, oper)){
							patternfound = 1;
						}
					}
					if(num5 == num6){
						if(table.comparetovalue(type, colvalue, pattern2, oper2)){
							pattern2found = 1;
						}
					}
					int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
					if(size == 0){
						colvalpos += colvalue.length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1 & pattern2found==1){
					table.seek(pos+cellpos);
					int cellsize = table.readShort()+6;
					for(int k = 0; k < cellsize; k++){
						table.seek(pos+cellpos+k);
						table.writeByte(0x00);
					}
					table.seek(newpos+(2*i));
					table.writeShort(0);
					if(i == num_cells-1){
						table.seek(pos+1);
						table.writeByte(num_cells-1);
					}
				}
			}
			
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}
		
		while(rightptr != -1);
	}
	
	/**
	 * @param val1
	 * @param num3
	 * @param num4
	 * @param val2
	 * @param val3
	 * @param val4
	 * @throws IOException
	 */
	public void update(String val1, int num3, int[] num4, String[] val2, String[] val3, String[] val4) throws IOException{
		int[] found = new int[val3.length];
		int position = findInitialLeaf() * pagesize;
		int cellpos = 0;
		int num7 = 0;
		int rightptr = table.readInt();
		
		int patternfound = 0;
		int string4 = 0;
		int suces = 0;
		do{
			table.seek(position+1);
			int num_cells = table.readByte();
			table.seek(position+4);
			rightptr = table.readInt();
			int newpos = position +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				string4 = 1;
				table.seek(newpos+(2*i));
				cellpos = table.readShort();
				if(cellpos == 0){
					continue;
				}
				table.seek(position+cellpos+6);
				num7 = table.readByte();
				table.seek(position+cellpos+2);
				Integer rowid = table.readInt();
				String colvalue = rowid.toString();
				for(int k=0; k < num4.length; k++){
					int ordinalnum = num4[k];
					if(string4 == ordinalnum){
						if(table.comparetovalue((byte)0x06, colvalue, val2[k], val3[k])){
							found[k] = 1;
							patternfound = 1;
						}
					}
				}
				int coltypepos = position+cellpos+7;
				int colvalpos = position+cellpos+7+num7;
				for(int j = 0; j < num7; j++){
					string4++;
					coltypepos = position+cellpos+7+j;
					table.seek(coltypepos);
					Byte type = table.readByte();
					table.seek(colvalpos);
					colvalue = table.readByType(type,1);
					for(int k=0; k < num4.length; k++){
						int ordinalnum = num4[k];
						if(string4 == ordinalnum){
							if(table.comparetovalue(type, colvalue, val2[k], val3[k])){
								found[k] = 1;
								patternfound = 1;
							}
						}
					}
					int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
					if(size == 0){
						colvalpos += colvalue.length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					suces = found[0];
					for(int k=1; k < found.length; k++){
						if(val4[k-1].equals("and")){
							suces = suces & found[k];
						}else{
							suces = suces | found[k];
						}
					}
					
					if(suces == 1){
						string4 = 1;
						table.seek(position+cellpos+6);
						num7 = table.readByte();
						table.seek(position+cellpos+2);
						if(num3 == string4){
							table.writeInt(Integer.parseInt(val1));
						}
						coltypepos = position+cellpos+7;
						colvalpos = position+cellpos+7+num7;
						for(int j = 0; j < num7; j++){
							string4++;
							coltypepos = position+cellpos+7+j;
							table.seek(coltypepos);
							Byte type = table.readByte();
							table.seek(colvalpos);
							colvalue = table.readByType(type,1);
							if(num3 == string4){
								table.seek(colvalpos);
								table.writeByType(val1, type);
							}
							int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
							if(size == 0){
								colvalpos += colvalue.length();
							}else{
								colvalpos += size;
							}
						}
					}
				}
			}
			position = (rightptr == -1) ? position :rightptr*pagesize;
		}while(rightptr != -1);
	}
	
	public int findAddLeafForCell(int rowid,String[] coltype, String[] colvalue, int cellsize,int pageno) throws IOException{
		int ret = 0;
		Byte pagetype = 0;
		if(pageno == -1){
			
		}else{
			int pos = (pageno)*pagesize;
			table.seek(pos);
			pagetype = table.readByte();
		}
		if(pagetype == 0){
			int newpageno = (int)table.length()/pagesize;
			table.setLength(table.length()+pagesize);
			PageDefinitions page = new PageDefinitions(table,rowid,coltype,colvalue,cellsize,newpageno,(byte)0x0D);
			page.writeLeafNodes();
			return newpageno;
		}else if(pagetype == 0x05){
			PageDefinitions page = new PageDefinitions(table);
			int leafnode = page.leafNode(rowid,pageno,cellsize);
			page = new PageDefinitions(table,rowid,coltype,colvalue,cellsize,leafnode,(byte)0x0D);
			ret = page.writeLeafNodes();
			if(ret == -1){
				int oldleafnode = leafnode;
				leafnode = findAddLeafForCell(rowid,coltype,colvalue,cellsize,-1);
				int pos = (oldleafnode)*pagesize;
				table.seek(pos+4);
				table.writeInt(leafnode);
			}
			return leafnode;
		}else{
			
		}
		
		return ret;
	}
	
	/**
	 * @param pageno
	 * @return
	 * @throws IOException
	 */
	public int findlastrowid(int pageno) throws IOException{
		int pos = (pageno)*pagesize;
		table.seek(pos);
		Byte pagetype = table.readByte();
		if(pagetype == 0){
			return 0;
		}else if(pagetype == 0x05){
			table.seek(pos+4);
			int rightpointer = table.readInt();
			if(rightpointer != -1){
				return findlastrowid(rightpointer);
			}else{
				table.seek(pos+2);
				short lastcellpos = table.readShort();
				table.seek(pos+lastcellpos);
				int leafpage = table.readInt();
				return findlastrowid(leafpage);
			}
		}else if(pagetype == 0x0D){
			table.seek(pos+1);
			short lastcellpos;
			int num_cells = table.readByte();
			while(true){
				table.seek(pos+8+(num_cells*2));
				lastcellpos = table.readShort();
				if(lastcellpos == 0){
					num_cells--;
				}
				else{
					break;
				}
				if(num_cells == 0){
					return -1;
				}
			}
			/*file.seek(pos+2);
			short lastcellpos = file.readShort();*/
			table.seek(pos+lastcellpos+2);
			return table.readInt();
		}
		
		return -1;
	}
	
	
	/**
	 * @param rowid
	 * @param coltype
	 * @param colvalue
	 * @param cellsize
	 * @param pageno
	 * @param leaf
	 * @throws IOException
	 */
	public void insert(int rowid,String[] coltype, String[] colvalue, int cellsize,int pageno,boolean leaf) throws IOException{
		if(!leaf){
			if(table.length() < ((pageno+1)*pagesize)){
				table.setLength((pageno+1)*pagesize);
			}

			if(intPosition(pageno)){
				int leafpageno = findAddLeafForCell(rowid,coltype,colvalue,cellsize,pageno);

				addintialCellValue(pageno,leafpageno,rowid);
			}
		}
	}
	
	public int findInitialLeaf() throws IOException{
		int pos = 0;
		int leafnode = 0;
		int cellpos = 0;
		while(true){
			table.seek(pos+8);
			cellpos = table.readShort();
			table.seek(pos+cellpos);
			leafnode = table.readInt();
			int newpos = leafnode * pagesize;
			table.seek(newpos);
			int pagetype = table.readByte();
			if(pagetype == 0x0D){
				break;
			}else{
				pos = newpos;
			}
		}
		
		return leafnode;
	}
	
	public List<String[]> findHeader(String table_name) throws IOException{
		int pos = findInitialLeaf() * pagesize;
		table.seek(pos+4);
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = table.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		boolean cellfound = false;
		boolean cellpassed = false;
		do{
			table.seek(pos+1);
			int num_cells = table.readByte();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				table.seek(newpos+(2*i));
				cellpos = table.readShort();
				if(cellpos == 0){
					continue;
				}
				table.seek(pos+cellpos+6);
				num_colms = table.readByte();
				table.seek(pos+cellpos+7);
				int tblnamesize = table.readByte() - 0x0C;
				int tblnamepos = pos+cellpos+7+num_colms;
				table.seek(tblnamepos);
				String tblname = table.readString(tblnamesize);
				if(tblname.equals(table_name)){
					colvalue = new String[num_colms+1];
					table.seek(pos+cellpos+2);
					Integer rowid = table.readInt();
					colvalue[0] = rowid.toString();
					int coltypepos = pos+cellpos+7;
					int colvalpos = pos+cellpos+7+num_colms;
					for(int j = 0; j < num_colms; j++){
						coltypepos = pos+cellpos+7+j;
						table.seek(coltypepos);
						Byte type = table.readByte();
						table.seek(colvalpos);
						colvalue[j+1] = table.readByType(type,1);
						int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
						if(size == 0){
							colvalpos += colvalue[j+1].length();
						}else{
							colvalpos += size;
						}
					}
					list.add(colvalue);
					cellfound = true;
				}else if(cellfound == true){
					cellpassed = true;
				}
				
			}
			if(cellfound == true && cellpassed == true){
				break;
			}else{
				pos = (rightptr == -1) ? pos :rightptr;
			}
		}while(rightptr != -1);
		
		return list;
		
	}
	
	public List<String[]> findAll() throws IOException{
		int pos = findInitialLeaf() * pagesize;
		table.seek(pos+4);
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = table.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		do{
			table.seek(pos+1);
			int num_cells = table.readByte();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				table.seek(newpos+(2*i));
				cellpos = table.readShort();
				if(cellpos == 0){
					continue;
				}
				table.seek(pos+cellpos+6);
				num_colms = table.readByte();
				colvalue = new String[num_colms+1];
				table.seek(pos+cellpos+2);
				Integer rowid = table.readInt();
				colvalue[0] = rowid.toString();
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					coltypepos = pos+cellpos+7+j;
					table.seek(coltypepos);
					Byte type = table.readByte();
					table.seek(colvalpos);
					colvalue[j+1] = table.readByType(type,1);
					int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
					if(size == 0){
						colvalpos += colvalue[j+1].length();
					}else{
						colvalpos += size;
					}
				}
				list.add(colvalue);
				
			}
			pos = (rightptr == -1) ? pos :rightptr;
		}while(rightptr != -1);

		return list;
	}
		
	public List<String[]> findCollmnEntry(String pattern) throws IOException{
		int pos = findInitialLeaf() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = table.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		int patternfound = 0;
		do{
			table.seek(pos+1);
			int num_cells = table.readByte();
			table.seek(pos+4);
			rightptr = table.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				table.seek(newpos+(2*i));
				cellpos = table.readShort();
				if(cellpos == 0){
					continue;
				}
				table.seek(pos+cellpos+6);
				num_colms = table.readByte();
				colvalue = new String[num_colms+1];
				table.seek(pos+cellpos+2);
				Integer rowid = table.readInt();
				colvalue[0] = rowid.toString();
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					coltypepos = pos+cellpos+7+j;
					table.seek(coltypepos);
					Byte type = table.readByte();
					table.seek(colvalpos);
					colvalue[j+1] = table.readByType(type,1);
					if(colvalue[j+1].equals(pattern)){
						patternfound = 1;
					}
					int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
					if(size == 0){
						colvalpos += colvalue[j+1].length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					list.add(colvalue);
				}else{
					continue;
				}
				
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);

		return list;
		
	}
	
	public List<String[]> findColumnEntry(String pattern,int ordinalnum, String oper) throws IOException{
		int pos = findInitialLeaf() * pagesize;
		int cellpos = 0;
		int num_colms = 0;
		int rightptr = table.readInt();
		List<String[]> list = new ArrayList<String[]>();
		String colvalue[];
		int patternfound = 0;
		int ordnumcheck = 0;
		do{
			table.seek(pos+1);
			int num_cells = table.readByte();
			table.seek(pos+4);
			rightptr = table.readInt();
			int newpos = pos +8;
			cellpos = 0;
			for(int i = 0; i < num_cells; i++){
				patternfound = 0;
				ordnumcheck = 1;
				table.seek(newpos+(2*i));
				cellpos = table.readShort();
				if(cellpos == 0){
					continue;
				}
				table.seek(pos+cellpos+6);
				num_colms = table.readByte();
				colvalue = new String[num_colms+1];
				table.seek(pos+cellpos+2);
				Integer rowid = table.readInt();
				colvalue[0] = rowid.toString();
				if(ordnumcheck == ordinalnum){
					if(table.comparetovalue((byte)0x06, colvalue[0], pattern, oper)){
						patternfound = 1;
					}
				}
				int coltypepos = pos+cellpos+7;
				int colvalpos = pos+cellpos+7+num_colms;
				for(int j = 0; j < num_colms; j++){
					ordnumcheck++;
					coltypepos = pos+cellpos+7+j;
					table.seek(coltypepos);
					Byte type = table.readByte();
					table.seek(colvalpos);
					colvalue[j+1] = table.readByType(type,1);
					if(ordnumcheck == ordinalnum){
						if(table.comparetovalue(type, colvalue[j+1], pattern, oper)){
							patternfound = 1;
						}
					}
					int size = DataTypeDefinitions.getSize(DataTypeDefinitions.getByteValue(type));
					if(size == 0){
						colvalpos += colvalue[j+1].length();
					}else{
						colvalpos += size;
					}
				}
				if(patternfound == 1){
					list.add(colvalue);
				}else{
					continue;
				}
				
			}
			pos = (rightptr == -1) ? pos :rightptr*pagesize;
		}while(rightptr != -1);

		return list;
		
	}
}


