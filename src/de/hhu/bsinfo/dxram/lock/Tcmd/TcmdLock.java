package de.hhu.bsinfo.dxram.lock.Tcmd;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.lock.AbstractLockService;
import de.hhu.bsinfo.dxram.lock.AbstractLockService.ErrorCode;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TcmdLock extends AbstractTerminalCommand{

	private static final Argument MS_ARG_CID = new Argument("cid", null, true, "Chunk ID");
	private static final Argument MS_ARG_LID = new Argument("lid", null, true, "Local Chunk ID");
	private static final Argument MS_ARG_NID = new Argument("nid", null, true, "Node ID");
	
	
	@Override
	public String getName() {
		
		return "chunklock";
	}

	@Override
	public String getDescription() {
		
		return "locks a chunk by either CID or PID and NID";
	}

	@Override
	public void registerArguments(ArgumentList p_arguments) {
		
		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_LID);
		p_arguments.setArgument(MS_ARG_NID);	
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		
		Long 	cid   = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		Long 	lid   = p_arguments.getArgumentValue(MS_ARG_LID, Long.class);
		Short 	nid   = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		
		
		ChunkService  chunkService	= getTerminalDelegate().getDXRAMService(ChunkService.class);
		AbstractLockService   lockService	= getTerminalDelegate().getDXRAMService(AbstractLockService.class);
		
		
		if (__checkID(cid, nid, lid))	// check if size, cid and lid are valid
			return false;						// if the values are not valid the function will do nothing and returns
		
		cid = __getCid(cid, lid, nid);
		
		Chunk chunk = chunkService.get(new long[] {cid})[0];
		
		if(chunk == null)
		{
			System.out.println("Getting Chunk with id '"+ Long.toHexString(cid) +"' failed");
			return false;
		}
		
		ErrorCode err = lockService.lock(true, 0, chunk);
		
		System.out.println("Returncode: " + err.toString());
		
		return true;
	}

	
	
	
	private long __getCid(Long cid, Long lid, Short nid)
	{
		BootService   bootService 	= getTerminalDelegate().getDXRAMService(BootService.class);
		// we favor full cid
		// take lid
		if (cid == null)
		{
			if (nid == null) 
				nid = bootService.getNodeID();
			
			// create cid
			cid = ChunkID.getChunkID(nid, lid);
		}
		
		return cid;
	}
	
	private boolean __checkID(Long cid, Short nid, Long lid)
	{
		
		if (cid == null && (lid == null || nid == null))
		{
			System.out.println("Error: Neither CID nor NID and LID specified");
			return true;
		}
		return false;
	}
	
}
