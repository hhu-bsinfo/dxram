package de.hhu.bsinfo.dxram.boot.tcmds;

import java.util.List;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.term.AbstractTerminalCommand;
import de.hhu.bsinfo.utils.args.ArgumentList;

public class TcmdNodeWait extends AbstractTerminalCommand {

	private static final ArgumentList.Argument MS_ARG_SUPERPEERS =
			new ArgumentList.Argument("superpeers", "0", true, "Number of available superpeers to wait for");
	private static final ArgumentList.Argument MS_ARG_PEERS =
			new ArgumentList.Argument("peers", "0", true, "Number of available peers to wait for");
	private static final ArgumentList.Argument MS_ARG_POLL_INTERVAL_MS =
			new ArgumentList.Argument("pollIntervalMs", "1000", true, "Polling interval when checking online status");

	@Override
	public String getName() {
		return "nodewait";
	}

	@Override
	public String getDescription() {
		return "Wait for a minimum number of nodes to be available/online";
	}

	@Override
	public void registerArguments(final ArgumentList p_arguments) {
		p_arguments.setArgument(MS_ARG_SUPERPEERS);
		p_arguments.setArgument(MS_ARG_PEERS);
		p_arguments.setArgument(MS_ARG_POLL_INTERVAL_MS);
	}

	@Override
	public boolean execute(final ArgumentList p_arguments) {

		short numSuperpeers = p_arguments.getArgumentValue(MS_ARG_SUPERPEERS, Short.class);
		short numPeers = p_arguments.getArgumentValue(MS_ARG_PEERS, Short.class);
		int pollIntervalMs = p_arguments.getArgumentValue(MS_ARG_POLL_INTERVAL_MS, Integer.class);

		BootService boot = getTerminalDelegate().getDXRAMService(BootService.class);

		getTerminalDelegate()
				.println("Waiting for at least " + numSuperpeers + " superpeers and " + numPeers + " peers...");

		List<Short> superpeers = boot.getOnlineSuperpeerNodeIDs();
		while (superpeers.size() < numSuperpeers) {
			try {
				Thread.sleep(pollIntervalMs);
			} catch (final InterruptedException ignored) {
			}

			superpeers = boot.getOnlineSuperpeerNodeIDs();
		}

		List<Short> peers = boot.getOnlinePeerNodeIDs();
		while (peers.size() < numPeers) {
			try {
				Thread.sleep(pollIntervalMs);
			} catch (final InterruptedException ignored) {
			}

			peers = boot.getOnlinePeerNodeIDs();
		}

		getTerminalDelegate().println(numSuperpeers + " superpeers and " + numPeers + " peers online");

		return true;
	}
}
