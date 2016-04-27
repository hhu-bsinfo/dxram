
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;

/**
 * Terminal command to list all currently available compute groups.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class TcmdMSGroupList extends AbstractTerminalCommand {
	@Override
	public String getName() {
		return "compgrouplist";
	}

	@Override
	public String getDescription() {
		return "Get a list of available compute groups";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {

	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		MasterSlaveComputeService masterSlaveComputeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		ArrayList<Pair<Short, Byte>> masters = masterSlaveComputeService.getMasters();

		System.out.println("List of available compute groups with master nodes (" + masters.size() + "):");
		for (Pair<Short, Byte> entry : masters) {
			System.out.println(entry.second() + ": " + NodeID.toHexString(entry.first()));
		}

		return true;
	}
}
