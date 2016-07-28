
package de.hhu.bsinfo.dxram.migration.tcmd;

import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.migration.MigrationService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TcmdMigrate extends AbstractTerminalCommand {

	private static final Argument MS_ARG_CID =
			new Argument("cid", null, true, "Full chunk ID of the chunk to get data from");
	private static final Argument MS_ARG_LID =
			new Argument("lid", null, true, "Separate local id part of the chunk to get data from");
	private static final Argument MS_ARG_NID =
			new Argument("nid", null, true, "Separate node id part of the chunk to get data from");

	private static final Argument MS_ARG_TARGET =
			new Argument("targetNid", null, false, "node to where to migrate the chunk");

	@Override
	public String getName() {

		return "migrate";
	}

	@Override
	public String getDescription() {

		return "migrates chunk data";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {

		p_arguments.setArgument(MS_ARG_CID);
		p_arguments.setArgument(MS_ARG_LID);
		p_arguments.setArgument(MS_ARG_NID);
		p_arguments.setArgument(MS_ARG_TARGET);

	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		Long cid = p_arguments.getArgumentValue(MS_ARG_CID, Long.class);
		Long lid = p_arguments.getArgumentValue(MS_ARG_LID, Long.class);
		Short nid = p_arguments.getArgumentValue(MS_ARG_NID, Short.class);
		Short target = p_arguments.getArgumentValue(MS_ARG_TARGET, Short.class);

		long chunkId = -1;
		// we favor full cid
		if (cid != null) {
			chunkId = cid;
		} else {
			if (lid != null) {
				if (nid == null) {
					System.out.println("error: missing nid for lid");
					return false;
				}
				// create cid
				chunkId = ChunkID.getChunkID(nid, lid);
			} else {
				System.out.println("No cid or nid/lid specified.");
				return false;
			}
		}

		MigrationService migrationService = getTerminalDelegate().getDXRAMService(MigrationService.class);

		migrationService.targetMigrate(chunkId, target);

		return true;
	}

}
