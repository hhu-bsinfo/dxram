
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService.StatusMaster;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TcmdMSMasterStatus extends AbstractTerminalCommand {
	private static final Argument MS_ARG_NODEID =
			new Argument("nodeid", null, false, "Node id of the master to get the status from");

	@Override
	public String getName() {
		return "compmasterstatus";
	}

	@Override
	public String getDescription() {
		return "Get the current status of a compute master node";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_NODEID);
	}

	@Override
	public boolean execute(ArgumentList p_arguments) {
		Short nodeID = p_arguments.getArgumentValue(MS_ARG_NODEID, Short.class);

		MasterSlaveComputeService masterSlaveComputeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		StatusMaster status = masterSlaveComputeService.getStatusMaster(nodeID);
		if (status == null) {
			System.out.println("Getting compute master status of node " + NodeID.toHexString(nodeID) + " failed");
			return true;
		}

		System.out.println("Status of master " + NodeID.toHexString(nodeID) + ":");
		System.out.println(status);

		return true;
	}
}
