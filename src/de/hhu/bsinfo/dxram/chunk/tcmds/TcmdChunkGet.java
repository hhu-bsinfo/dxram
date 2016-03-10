package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdChunkGet extends TerminalCommand{
	
	private final static String MS_ARG_CID = "cid";
	private final static String MS_ARG_LID = "lid";
	private final static String MS_ARG_SIZ = "size";
	private final static String MS_ARG_OFF = "offset";
	private final static String MS_ARG_LEN = "length";
	private final static String MS_ARG_HEX = "printHex";
	
	
	
	@Override
	public String getName() 
	{
		return "chunkget";
	}
	

	@Override
	public String getUsageMessage() 
	{
		return 	  "chunkget cid[long]:CID "
				+ "size[int]:SIZ "
				+ "[offset[int]:OFF] "
				+ "[length[int]:LEN] "
				+ "[printHex[boolean]:HEX]";
//				+ " or "
//				+ "chunkget lid[long]:LID "
//				+ "[nid[short]:NID] "
//				+ "[offset[int]: OFF] "
//				+ "[length[int]: LEN] "
//				+ "[printHex[boolean]: HEX]";
	}

	@Override
	public String getHelpMessage()
	{
		return "Searches chunk which matches the specified CID";
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		Long 	cid   = p_arguments.getArgumentValue(MS_ARG_CID);
		Long 	lid   = p_arguments.getArgumentValue(MS_ARG_LID);
		Integer	size  = p_arguments.getArgumentValue(MS_ARG_SIZ);
		Integer	off   = p_arguments.getArgumentValue(MS_ARG_OFF);
		Integer	len   = p_arguments.getArgumentValue(MS_ARG_LEN);
		Boolean isHex = p_arguments.getArgumentValue(MS_ARG_HEX);
		
		if(cid == null && lid == null)
			return false;
		
		ChunkService  chunkService	= getTerminalDelegate().getDXRAMService(ChunkService.class);
		BootComponent bootComp		= getTerminalDelegate().getDXRAMComponent(BootComponent.class);
		BootService   bootService 	= getTerminalDelegate().getDXRAMService(BootService.class);
		
		if(size == null)
		{
			size = 1024*1024*16;
			System.out.println("No Size specified! Going with default size of "+size);
		}
		
		Chunk chunk;
		
		// we favor full cid
		if (cid != null)
		{
			chunk = new Chunk(cid, size); // Todo?
			
		}else // take lid
		{
			bootService.getNodeID();
			chunk = new Chunk(lid, size);
		}
		
		
		int num = chunkService.get(chunk);
		
		if(num == 0)
		{
			System.out.println("Getting Chunk with id '"+ cid +"' failed");
		}else
		{
			if(isHex)
				System.out.println("To Do punch Mike");
			else
			{
				String data = chunk.getData().toString();
				System.out.println(data);
			}	
		}	
		
		return true;
	}

}
