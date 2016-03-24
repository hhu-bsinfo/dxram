package de.hhu.bsinfo.dxram.chunk.tcmds;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TcmdChunkList extends TerminalCommand{

	private static final Argument MS_ARG_NID = new Argument("nid", null, true, "Node ID");
	private static final Argument MS_ARG_START = new Argument("start", "0", true, "start of range to list");
	private static final Argument MS_ARG_END = new Argument("end", null, false, "start of range to list");
	
	
	
	@Override
	public String getName() {
		
		return "chunklist";
	}

	@Override
	public String getDescription() {
		
		return "lists a range of nodes";
	}

	@Override
	public void registerArguments(ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_START);
		p_arguments.setArgument(MS_ARG_END);		
		p_arguments.setArgument(MS_ARG_NID);
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		
		
		Long start = p_arguments.getArgumentValue(MS_ARG_START, Long.class);
		Long end   = p_arguments.getArgumentValue(MS_ARG_END, Long.class);
		Short nid   = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		
		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);
		
		ArrayList<Long> chunkRanges;
		
		if(nid == null)
			chunkRanges = chunkService.getAllLocalChunkIDRanges();
		else
			chunkRanges = chunkService.getAllLocalChunkIDRanges(nid);
		
		
		for(int i = 0; i < chunkRanges.size(); i ++)
		{
			long currRange = chunkRanges.get(i);
			System.out.println("Range " + i + " :  " + Long.toHexString(currRange));
		}
		
		
		
		return true;
	}

	
}
