package de.hhu.bsinfo.dxram.boot.tcmds;

import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.term.TerminalCommand;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdNodeList extends TerminalCommand {

	private static final String MS_ARG_NODE_ID = "role";
	
	@Override
	public String getName() {
		return "nodelist";
	}

	@Override
	public String getUsageMessage() {
		return "nodelist [role[str]:ROLE]";
	}

	@Override
	public String getHelpMessage() {
		return "List all available nodes or nodes of a specific type.\n"
				+ "role: filter by role specified.";
	}

	@Override
	public boolean execute(ArgumentList p_arguments) 
	{
		String strRole = p_arguments.getArgumentValue(MS_ARG_NODE_ID);
		if (strRole != null)
		{
			NodeRole roleFilter = NodeRole.toNodeRole(strRole);
			BootComponent boot = getTerminalDelegate().getDXRAMComponent(BootComponent.class);
			List<Short> nodeIDs = boot.getAvailableNodeIDs();
			System.out.println("Filtering by role " + roleFilter);
			System.out.println("Total available nodes (" + nodeIDs.size() + "):");
			for (short nodeId : nodeIDs) {
				NodeRole curNodeRole = boot.getNodeRole(nodeId);
				if (roleFilter.equals(curNodeRole)) {
					System.out.println("\t" + nodeId + ", " + curNodeRole + ", " + boot.getNodeAddress(nodeId));
				}
			}
		}
		else
		{
			BootComponent boot = getTerminalDelegate().getDXRAMComponent(BootComponent.class);
			List<Short> nodeIDs = boot.getAvailableNodeIDs();
			System.out.println("Available nodes (" + nodeIDs.size() + "):");
			for (short nodeId : nodeIDs) {
				System.out.println("\t" + nodeId + ", " + boot.getNodeRole(nodeId) + ", " + boot.getNodeAddress(nodeId));
			}
		}
		
		return true;
	}

}
