package com.databaseEngine;

import java.io.IOException;


public class PageDefinitions {
	
    TableDefinitions tableName;
	public int rowName;
	public int page;
	public String[] column;
	public String[] colVal;
	public int size;
	public int pageNum;
	static int sizeofPage = 512;
	
	public PageDefinitions(TableDefinitions tableName){
		this.tableName = tableName;
	}
	
	public PageDefinitions(TableDefinitions tableName,int rowName,String[] column, String[] colVal, int size,int pageNum,int pagetype){
		this.tableName = tableName;
		this.column = column;
		this.colVal = colVal;
		this.pageNum = pageNum;
		this.rowName = rowName;
		this.size = size;
		this.page = pagetype;
	}

	public PageDefinitions(TableDefinitions tableName, int pageNum, String[] colVal, int pagetype){
		this.tableName = tableName;
		this.pageNum = pageNum;
		this.colVal = colVal;
		this.page = pagetype;
	}
	
	/**
	 * @throws IOException
	 */
	public int writeLeafNodes() throws IOException{
		int currentPosition = pageNum * sizeofPage;
		tableName.seek(currentPosition);
		Byte currentPageType = tableName.readByte();
		short sizePl=0;
		short cellpos=0;
		if(currentPageType == 0){
			tableName.seek(currentPosition);
			tableName.writeByte(page);
			tableName.seek(currentPosition+1);
			tableName.writeByte(0x01);
			tableName.seek(currentPosition+4);
			tableName.writeInt(-1);
			sizePl = (short)(column.length+size+1);
			cellpos = (short)(sizeofPage - (6 + sizePl));		
		}else{
			int numa = 0;
			tableName.seek(currentPosition+1);
			numa = tableName.readByte();
			tableName.seek(currentPosition+2);
			cellpos = tableName.readShort();
			if(cellpos < (size+column.length+6+1+8+((numa+1)*2))){
				return -1;
			}
			tableName.seek(currentPosition+1);
			numa = tableName.readByte();
			tableName.seek(currentPosition+1);
			tableName.writeByte(numa+1);
			cellpos -= (size+column.length+6+1);
			sizePl = (short)(column.length+size+1);
		}

		tableName.seek(currentPosition+8);
		int j = 0;
		while(tableName.readShort() != 0){
			j +=2;
			tableName.seek(currentPosition+8+j);
		}
		tableName.seek(currentPosition+8+j);
		tableName.writeShort(cellpos);
		tableName.seek(currentPosition+2);
		tableName.writeShort(cellpos);
		tableName.seek(currentPosition+cellpos);
		tableName.writeShort(sizePl);
		tableName.seek(currentPosition+cellpos+2);
		tableName.writeInt(rowName);
		tableName.seek(currentPosition+cellpos+6);
		tableName.writeByte(column.length);
		for(int i=0; i < column.length; i++){
			Byte type = DataTypeDefinitions.getDataType(column[i]);
			if(type == 0x0C){
				int len = colVal[i].length() & 0xFF;
				type = (byte) (type + (len & 0xFF));
			}
			tableName.seek(currentPosition+cellpos+7+i);
			tableName.writeByte(type);
		}
		int newpos = currentPosition +cellpos+7+column.length;
		for(int i = 0; i < colVal.length; i++){
			tableName.seek(newpos);
			tableName.writeByType(colVal[i], DataTypeDefinitions.getDataType(column[i]));
			newpos += DataTypeDefinitions.getSize(column[i]);
			if(DataTypeDefinitions.getDataType(column[i]) == 0x0C){
				newpos += colVal[i].length();
			}
		}
		return 1;
	}

	/**
	 * @param cellrowid
	 * @param leafpageno
	 * @throws IOException
	 */
	public void writeinterior(int cellrowid,int leafpageno) throws IOException{
		int pos = pageNum * sizeofPage;
		tableName.seek(pos);
		Byte curpagetype = tableName.readByte();
		short cellpos = 0;
		int currleafno = 0;
		int currrowid = 0;
		if(curpagetype != 0x05){
			tableName.seek(pos);
			tableName.writeByte(page);
			tableName.seek(pos+1);
			tableName.writeByte(0x01);
			tableName.seek(pos+4);
			tableName.writeInt(-1);
			cellpos = (short)(sizeofPage - 10);
		}else{
			tableName.seek(pos+4);
			int rightptr = tableName.readInt();
			if(leafpageno == rightptr){
				return;
			}
			tableName.seek(pos+2);
			cellpos = tableName.readShort();
			if(rightptr != -1){
				currleafno = rightptr;
				int newpos = currleafno * sizeofPage;
				tableName.seek(newpos+2);
				int newcellpos = tableName.readShort();
				tableName.seek(newpos+newcellpos+2);
				currrowid = tableName.readInt();
				cellpos -= 8;
				tableName.seek(pos+cellpos);
				tableName.writeInt(rightptr);
				tableName.seek(pos+cellpos+4);
				tableName.writeInt(currrowid);
				tableName.seek(pos+4);
				tableName.writeInt(leafpageno);
				return;
				
			}
			
			else
			{
				tableName.seek(pos+2);
				cellpos = tableName.readShort();
				tableName.seek(pos+cellpos);
				currleafno = tableName.readInt();
				tableName.seek(pos+cellpos+4);
				currrowid = tableName.readInt();
				if(currleafno == leafpageno){
					if(cellrowid > currrowid){
						tableName.seek(pos+cellpos+4);
						tableName.writeByType(colVal[1], DataTypeDefinitions.getDataType(DataTypeDefinitions.INT));
					}
					return;
					
					
				}
				else
				
				{
					tableName.seek(pos+4);
					rightptr = tableName.readInt();
					if(rightptr == -1){
						tableName.seek(pos+4);
						tableName.writeInt(leafpageno);
					}
					return;
				}
			}
		}
		
		
		tableName.seek(pos+8);
		int j = 0;
		while(tableName.readByte() != 0)
		{
			j +=2;
			tableName.seek(pos+8+j);
		}
		tableName.seek(pos+8+j);
		tableName.writeShort(cellpos);
		tableName.seek(pos+2);
		tableName.writeShort(cellpos);
		tableName.seek(pos+cellpos);
		tableName.writeByType(colVal[0], DataTypeDefinitions.getDataType(DataTypeDefinitions.INT));
		tableName.writeByType(colVal[1], DataTypeDefinitions.getDataType(DataTypeDefinitions.INT));
	}
	
	
	
	/**
	 * @param a
	 * @param b
	 * @param size
	 * @return
	 * @throws IOException
	 */
	public int leafNode(int a,int b,int size) throws IOException
	{
		pageNum = b;
		rowName = a;
		
		int pos = pageNum * sizeofPage;
		tableName.seek(pos+1);
		int num_cells = tableName.readByte();
		int leafVal = 0;
		int cellpos = 0;
		int cellrowid = 0;
		tableName.seek(pos+4);
		int found = 0;
		int rightptr = tableName.readInt();
		tableName.seek(pos+8);
		for(int i = 0; i < num_cells; i++){
			tableName.seek(pos+8+(2*i));
			cellpos = tableName.readShort();
			if(cellpos == 0){
				continue;
			}
			tableName.seek(pos+cellpos);
			leafVal = tableName.readInt();
			tableName.seek(pos+cellpos+4);
			cellrowid = tableName.readInt();
			if(rowName > cellrowid){
				continue;
			}
			else
			{
				found = 1;
				break;
			}
		}
		if(found == 0)
		{
			if(rightptr!= -1)
			{
				return rightptr;
			}
		}
		
		
		return leafVal;	
		
	}

}
