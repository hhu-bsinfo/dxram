
package de.hhu.bsinfo.dxram.recovery.tcmds;

import de.hhu.bsinfo.dxram.recovery.RecoveryService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;

public class TcmdRecover extends AbstractTerminalCommand {

	private static final Argument MS_ARG_OWNER = new Argument("owner", null, false, "Node ID");
	private static final Argument MS_ARG_DEST = new Argument("dest", null, false, "Node ID");

	@Override
	public String getName() {
		return "recover";
	}

	@Override
	public String getDescription() {
		return "Recovers all data of owner on dest";

	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_OWNER);
		p_arguments.setArgument(MS_ARG_DEST);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {
		Short owner = p_arguments.getArgumentValue(MS_ARG_OWNER, Short.class);
		Short dest = p_arguments.getArgumentValue(MS_ARG_DEST, Short.class);

		RecoveryService recoveryService = getTerminalDelegate().getDXRAMService(RecoveryService.class);

		recoveryService.recover(owner, dest, true);

		return true;
	}

}
