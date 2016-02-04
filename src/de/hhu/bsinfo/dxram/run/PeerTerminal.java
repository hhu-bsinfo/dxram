package de.hhu.bsinfo.dxram.run;

import de.hhu.bsinfo.dxram.term.TerminalService;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.utils.main.Main;
import de.hhu.bsinfo.utils.main.MainArguments;

// note: don't run this in eclipse like everything else
// create new run configuration with:
// --geometry 250x60 -e "java -Ddxram.network.ip=127.0.0.1 -Ddxram.network.port=22220 -Ddxram.role=Peer -Dlog4j.configuration=file://///home/nothaas/Workspace/workspace_dxram/dxram/config/log4j.properties -cp lib/slf4j-log4j12-1.6.1.jar:lib/slf4j-api-1.6.1.jar:lib/zookeeper-3.4.3.jar:lib/log4j-1.2.16.jar:bin/ de.hhu.bsinfo.dxram.run.PeerTerminal"
public class PeerTerminal extends DXRAMMain {
	
	public static void main(final String[] args) {
		Main main = new PeerTerminal();
		main.run(args);
	}
	
	protected PeerTerminal()
	{
		super(null, null, NodeRole.PEER);
	}

	@Override
	protected int mainApplication(MainArguments p_arguments) {
		TerminalService term = getService(TerminalService.class);
		if (term == null) {
			System.out.println("Cannot run terminal, missing service.");
			return -1;
		}
		
		System.out.println("PeerTerminal started.");
		
		term.loop();
		return 0;
	}
}
