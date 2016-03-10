package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdChunkPut extends TerminalCommand{
	
	private final static String MS_ARG_CID = "cid";
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
		return "chunkput cid[long]:CID data[String]:STR";
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
		String 	data  = p_arguments.getArgumentValue(MS_ARG_DAT);
		
		if(cid == null || data == null)
			return false;
		
		ChunkService  chunkService	= getTerminalDelegate().getDXRAMService(ChunkService.class);
		BootComponent bootComp		= getTerminalDelegate().getDXRAMComponent(BootComponent.class);
		BootService   bootService 	= getTerminalDelegate().getDXRAMService(BootService.class);
		
		Chunk chunk = new Chunk(cid, DFLT_SIZE); // Todo?
		
		chunkService.get(chunk);
		chunk.getData().put(data.getBytes());
		
		return true;
	}
}
