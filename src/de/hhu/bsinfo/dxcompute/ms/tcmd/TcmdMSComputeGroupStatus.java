
package de.hhu.bsinfo.dxcompute.ms.tcmd;

import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService;
import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService.StatusMaster;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

/**
 * Terminal command to get the current status of a compute group.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class TcmdMSComputeGroupStatus extends AbstractTerminalCommand {
	private static final Argument MS_ARG_CGID =
			new Argument("cgid", null, false, "Compute group id to get the status from");

	@Override
	public String getName() {
		return "compgroupstatus";
	}

	@Override
	public String getDescription() {
		return "Get the current status of a compute group";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_CGID);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short cgid = p_arguments.getArgumentValue(MS_ARG_CGID, Short.class);

		MasterSlaveComputeService masterSlaveComputeService =
				getTerminalDelegate().getDXRAMService(MasterSlaveComputeService.class);

		StatusMaster status = masterSlaveComputeService.getStatusMaster(cgid);
		if (status == null) {
			System.out.println("Getting compute group status of group " + cgid + " failed");
			return true;
		}

		System.out.println("Status of group " + cgid + ":");
		System.out.println(status);

		return true;
	}
}
