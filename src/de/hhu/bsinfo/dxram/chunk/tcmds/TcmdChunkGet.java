package de.hhu.bsinfo.dxram.chunk.tcmds;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import javax.print.DocFlavor.BYTE_ARRAY;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

// TODO mike refactoring: refer to chunk create/remove commands
public class TcmdChunkGet extends TerminalCommand{
	
	private static final Argument MS_ARG_CID = new Argument("cid", null, true, "Chunk ID");
	private static final Argument MS_ARG_LID = new Argument("lid", null, true, "Local Chunk ID");
	private static final Argument MS_ARG_NID = new Argument("nid", null, true, "Node ID");
	private static final Argument MS_ARG_OFF = new Argument("offset", "0", true, "offset to read data from chunk");
	private static final Argument MS_ARG_LEN = new Argument("length", null, true, "how much to read from data");
	private static final Argument MS_ARG_HEX = new Argument("isHex", "false", true, "print in HEX?");
	
	
	@Override
	public String getName() 
	{
		return "chunkget";
	}

	@Override
	public String getDescription()
	{
		return "Searches chunk which matches the specified CID";

	}
	
	@Override
	public void registerArguments(final ArgumentList p_arguments)
	{
		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_LID);
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_OFF);
		p_arguments.setArgument(MS_ARG_LEN);
		p_arguments.setArgument(MS_ARG_HEX);
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {

		Long 	cid   = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		Long 	lid   = p_arguments.getArgumentValue(MS_ARG_LID, Long.class);
		Short 	nid   = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		Integer	off   = p_arguments.getArgumentValue(MS_ARG_OFF, Integer.class);
		Integer	len   = p_arguments.getArgumentValue(MS_ARG_LEN, Integer.class);
		Boolean isHex = p_arguments.getArgumentValue(MS_ARG_HEX, Boolean.class);
		
		
		ChunkService  chunkService	= getTerminalDelegate().getDXRAMService(ChunkService.class);
		
		if (__checkID(cid, nid, lid))	// check if size, cid and lid are valid
			return false;						// if the values are not valid the function will do nothing and returns
		
		cid = __getCid(cid, lid, nid);
		
		Chunk chunk = chunkService.get(new long[] {cid})[0]; 
		
		if(chunk == null)
		{
			System.out.println("Getting Chunk with id '"+ Long.toHexString(cid) +"' failed");
			return false;
		}
		
		
		 byte[] chunkData 		= chunk.getData().array();
		 int 	lengthOfData 	= chunkData.length;
		 int 	lengthOfRead	= lengthOfData;
		 
		 if (len != null)	
			 lengthOfRead = len;
		 
		 if (__checkLengthAndOffset(lengthOfData, off, lengthOfRead))	// check for valid length and offset input
			 return false;

		 if (lengthOfData - off < lengthOfRead)
			 lengthOfRead = lengthOfData - off;

		 
		 byte[] readData = __crateByteArray(lengthOfRead, chunkData, off);
		 
		 for (int i = 0; i < lengthOfRead; i++)
		 {
			readData[i] = chunkData[i + off]; 
		 }
		  
		 __printData(readData, isHex);
		 
		 return true;
	}
	
	
	private byte[] __crateByteArray(int lengthOfRead, byte[] chunkData, Integer offset)
	{
		 byte[] data = new byte[lengthOfRead];
		 
		 for (int i = 0; i < lengthOfRead; i++)
		 {
			 data[i] = chunkData[i + offset]; 
		 }
		 
		 return data;
	}
	
	
	private boolean __checkLengthAndOffset(int lengthOfData, int offset, int lengthOfRead)
	{
		if (lengthOfData < offset)	// check if offset is larger than data
		 {	
			 System.out.println("Error: specified offset is greater than the length of the chunk data\n");
			 return true;
		 }
		 
		 if (offset < 0)
		 {
			 System.out.println("Error specified offset is less than Zero!");
			 return true;
		 }
		 
			 
		 if (lengthOfRead < 0)
		 {
			 System.out.println("Error: Negative length specified!");
			 return true;
		 }
		 
		 return false;
	}

	private long __getCid(Long cid, Long lid, Short nid)
	{
		// we favor full cid
		
		if (cid == null)
		{			
			// create cid
			cid = ChunkID.getChunkID(nid, lid);
		}
		
		return cid;
	}
	
	// true if Error was found
	private boolean __checkID(Long cid, Short nid, Long lid)
	{
		
		if (cid == null && (lid == null || nid == null))
		{
			System.out.println("Error: Neither CID nor NID and LID specified");
			return true;
		}
		return false;
	}
	
	private void __printData(byte[] data, boolean printHex)
	{
		if(printHex)
		{
			StringBuilder output = new StringBuilder();
		    for (byte b : data) {
		    	output.append(String.format("%02X ", b));
		    }
		    System.out.println(output.toString());
		}else
		{
			String output = new String(data, StandardCharsets.US_ASCII);
			System.out.println(output);
		}
	}

}
