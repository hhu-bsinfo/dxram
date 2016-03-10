package de.hhu.bsinfo.dxram.chunk.tcmds;

import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdChunkPut extends TerminalCommand{
	
	private final static String MS_ARG_CID = "cid";
	private final static String MS_ARG_LID = "lid";
	private final static String MS_ARG_NID = "nid";
	private final static String MS_ARG_DAT = "data";
	private final static int 	DFLT_SIZE  = 1024*1024*16;
	
	@Override
	public String getName() 
	{
		return "chunkput";
	}
	
	@Override
	public String getUsageMessage() 
	{
		return 	  "chunkput cid[long]:CID data[Str]:STR"
				+ " or "
				+ "chunkput lid[long]:LID "
				+ "[nid[short]:NID] "
				+ "data[String]:STR";
	}
	
	@Override
	public String getHelpMessage() 
	{
		return 		"Put a String in the specified chunk."
				+ 	"If the specified string is too long it will be trunced";
	}
	
	@Override
	public boolean execute(ArgumentList p_arguments) 
	{
		
		Long 	cid   = p_arguments.getArgumentValue(MS_ARG_CID);
		Long 	lid   = p_arguments.getArgumentValue(MS_ARG_LID);
		Short 	nid   = p_arguments.getArgumentValue(MS_ARG_NID);
		String 	data  = p_arguments.getArgumentValue(MS_ARG_DAT);
		
		
		System.out.println("data:" + data);
		System.out.println("cid: "+ cid.longValue());
		
		if(cid == null && lid == null)
			return false;
		
		if(data == null)
			return false;
		
		ChunkService  chunkService	= getTerminalDelegate().getDXRAMService(ChunkService.class);
		BootComponent bootComp		= getTerminalDelegate().getDXRAMComponent(BootComponent.class);
		BootService   bootService 	= getTerminalDelegate().getDXRAMService(BootService.class);
		
		
		if(cid == null)
		{
			if (nid == null) {
				nid = bootComp.getNodeID();
			}	
			// create cid
			cid = ChunkID.getChunkID(nid, lid);
		}
		
		Chunk chunk = new Chunk(cid, DFLT_SIZE); // Todo Size
		
		int num = chunkService.get(chunk);
		if(num == 0)
			System.out.println("Getting Chunk with id '"+ cid +"' failed");
		else
			chunk.getData().put( data.getBytes(StandardCharsets.US_ASCII) );
		
		num = chunkService.put(chunk);
		if(num == 0)
			System.out.println("Putting Chunk with id '"+ cid +"' failed");
		
		return true;
	}
}
