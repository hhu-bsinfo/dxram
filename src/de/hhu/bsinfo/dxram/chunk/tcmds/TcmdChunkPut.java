package de.hhu.bsinfo.dxram.chunk.tcmds;

import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

//TODO mike refactoring: refer to chunk create/remove commands
public class TcmdChunkPut extends TerminalCommand{
		
	private static final Argument MS_ARG_CID = new Argument("cid", null, false, "Chunk ID");
	private static final Argument MS_ARG_DAT = new Argument("data", null, false, "Data string to store");
	private static final Argument MS_ARG_SIZ = new Argument("size", null, false, "Size of the specified chunk");
	
	
	@Override
	public String getName() 
	{
		return "chunkput";
	}
	
	@Override
	public String getDescription() 
	{

		return 		"Put a String in the specified chunk."
				+ 	"If the specified string is too long it will be trunced";
	}
	
	@Override
	public void registerArguments(final ArgumentList p_arguments)
	{
		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_DAT);
		p_arguments.setArgument(MS_ARG_SIZ);
	}
	
	@Override
	public boolean execute(ArgumentList p_arguments) 
	{
		Long 	cid   = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		String 	data  = p_arguments.getArgumentValue(MS_ARG_DAT, String.class);
		Integer	size  = p_arguments.getArgumentValue(MS_ARG_SIZ, Integer.class);
		
		System.out.println("data\n " + data + "\nput in "+ cid.longValue());
				
		ChunkService  chunkService	= getTerminalDelegate().getDXRAMService(ChunkService.class);
		
		
		Chunk chunk = new Chunk(cid, size); 
		
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
