
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import java.util.ArrayList;

import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.Pair;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdMSMasterList extends AbstractTerminalCommand {
	@Override
	public String getName() {
		return "compmasterlist";
	}

	@Override
	public String getDescription() {
		return "Get a list of available compute master nodes";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {

	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		MasterSlaveComputeService masterSlaveComputeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		ArrayList<Pair<Short, Byte>> masters = masterSlaveComputeService.getMasters();

		System.out.println("List of available compute master nodes (" + masters.size() + "):");
		for (Pair<Short, Byte> entry : masters) {
			System.out.println(NodeID.toHexString(entry.first()) + ": " + entry.second());
		}

		return true;
	}
}
