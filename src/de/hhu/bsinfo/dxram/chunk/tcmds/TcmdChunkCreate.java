package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdChunkCreate extends TerminalCommand {

	private final static String MS_ARG_SIZE = "size";
	private final static String MS_ARG_NODEID = "nodeID";
	
	@Override
	public String getName() {
		return "chunkcreate";
	}

	@Override
	public String getUsageMessage() {
		return "chunkcreate size[int]:nbytes [nodeID[short]:NID]";
	}

	@Override
	public String getHelpMessage() {
		return "Create a chunk either on the current node or a remote node specified by nodeID.";
	}

	@Override
	public boolean execute(ArgumentList p_arguments) 
	{
		Integer size = p_arguments.getArgument(MS_ARG_SIZE, Integer.class);
		Short nodeID = p_arguments.getArgument(MS_ARG_NODEID, Short.class);
		
		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);
		
		if (size == null) {
			System.out.println("Missing size parameter.");
			return false;
		}
		
		long[] chunkIDs = null;
		
		if (nodeID != null) {
			chunkIDs = chunkService.create(nodeID, size);
		} else {
			chunkIDs = chunkService.create(size);
		}
		
		if (chunkIDs != null) {
			System.out.println("Created chunk " + Long.toHexString(chunkIDs[0]));
		} else {
			System.out.println("Creating chunk failed.");
		}
		
		return true;
	}

}
