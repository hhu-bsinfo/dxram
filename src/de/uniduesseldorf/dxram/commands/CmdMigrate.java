package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.CommandMessage;

public class CmdMigrate extends Cmd {
	public static String STR_CMD = "migrate";
	public static String STR_UM  = "migrate ...";
	public static String STR_HM  = "Migrate an object from source node SNID to destionation node DNID";

	public CmdMigrate() {
		super(STR_CMD, STR_UM, STR_HM);
	}

	// called by shell
	public boolean areParametersSane (String arguments[]) {
		return true;
	}
	
	// called after parameter have been checked
	public int execute(String command) {
	//	new CommandMessage(Short.parseShort(arguments[1]), type, arguments).send(m_network);

		return 0;
	}
}