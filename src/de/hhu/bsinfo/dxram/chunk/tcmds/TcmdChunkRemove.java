package de.hhu.bsinfo.dxram.chunk.tcmds;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdChunkRemove extends TerminalCommand {

	private final static String MS_ARG_CID = "cid";
	private final static String MS_ARG_LID = "lid";
	private final static String MS_ARG_NID = "nid";
	
	@Override
	public String getName() {
		return "chunkremove";
	}

	@Override
	public String getUsageMessage() {
		return "chunkremove cid[long]:CID or chunkremove lid[long]:LID [nid[short]:NID]";
	}

	@Override
	public String getHelpMessage() {
		return "Remove an existing chunk. Usable with either full chunkID or split into LID and NID, where NID"
				+ " is optional. Not providing the NID will default to the current node's ID.";
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		Long cid = p_arguments.getArgumentValue(MS_ARG_CID);
		Long lid = p_arguments.getArgumentValue(MS_ARG_LID);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID);
		
		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);
		BootComponent bootComp = getTerminalDelegate().getDXRAMComponent(BootComponent.class);
		
		// we favor full cid
		if (cid != null)
		{
			if (chunkService.remove(cid) != 1)
			{
				System.out.println("Removing chunk with ID " + Long.toHexString(cid) + " failed.");
				return true;
			}
		}
		else
		{
			if (lid != null)
			{
				// check for remote id, otherwise we assume local
				if (nid == null) {
					nid = bootComp.getNodeID();
				}
				
				// create cid
				cid = ChunkID.getChunkID(nid, lid);
				if (chunkService.remove(cid) != 1)
				{
					System.out.println("Removing chunk with ID " + Long.toHexString(cid) + " failed.");
					return true;
				}
			}
			else
			{
				System.out.println("No cid or lid specified.");
				return false;
			}
		}
		
		System.out.println("Chunk " + Long.toHexString(cid) + " removed.");
		return true;
	}
}
