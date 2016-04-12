
package de.hhu.bsinfo.dxram.chunk.tcmds;

import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

//TODO: A terminal node does not store chunks
//TODO mike refactoring: refer to chunk create/remove commands
public class TcmdChunkPut extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CID = new Argument("cid", null, true, "Chunk ID");
	private static final Argument MS_ARG_LID = new Argument("lid", null, true, "Local Chunk ID");
	private static final Argument MS_ARG_NID = new Argument("nid", null, true, "Node ID");
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

		return "Put a String in the specified chunk."
				+ "If the specified string is too long it will be trunced";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments)
	{
		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_LID);
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_DAT);
		p_arguments.setArgument(MS_ARG_SIZ);
	}

	@Override
	public boolean execute(ArgumentList p_arguments)
	{
		Long cid = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		Long lid = p_arguments.getArgumentValue(MS_ARG_LID, Long.class);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		String data = p_arguments.getArgumentValue(MS_ARG_DAT, String.class);
		Integer size = p_arguments.getArgumentValue(MS_ARG_SIZ, Integer.class);

		ChunkService chunkService = getTerminalDelegate().getDXRAMService(ChunkService.class);

		if (__checkIDandSize(size, cid, lid))
		{
			return true; // if the values are not valid the function will do nothing and returns
		}

		System.out.println("Should not be visible");

		cid = __getCid(cid, lid, nid);

		Chunk chunk = new Chunk(cid, size);

		int num = chunkService.get(chunk);
		if (num == 0) {
			System.out.println("Getting Chunk with id '" + Long.toHexString(cid) + "' failed");
		} else {
			chunk.getData().put(data.getBytes(StandardCharsets.US_ASCII));
		}

		num = chunkService.put(chunk);
		if (num == 0) {
			System.out.println("Putting Chunk with id '" + Long.toHexString(cid) + "' failed");
		} else {
			System.out.println(data + " put in " + Long.toHexString(cid));
		}

		return true;
	}

	// this needs to be refactored in one helper class since these functions will be used throughout multiple classes

	private boolean __checkIDandSize(Integer size, Long cid, Long lid)
	{
		if (size < 0)
		{
			System.out.println("Error: specified size lower than Zero!");
			return true;
		}

		if (cid == null && lid == null)
		{
			System.out.println("Error: Neither CID nor LID specified");
			return true;
		}
		return false;
	}

	private long __getCid(Long cid, Long lid, Short nid)
	{
		BootService bootService = getTerminalDelegate().getDXRAMService(BootService.class);
		// we favor full cid
		// take lid
		if (cid == null)
		{
			if (nid == null) {
				nid = bootService.getNodeID();
			}

			// create cid
			cid = ChunkID.getChunkID(nid, lid);
		}

		return cid;
	}

}
