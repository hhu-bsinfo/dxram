package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

public class CmdChunks extends Cmd {
	public static String STR_CMD = "chunks";
	public static String STR_UM  = "chunks NID lastid|allids";
	public static String STR_HM  = "Get information about chunks for a given node.\n   lastid: highest used OID\n   allids: all stored OIDs";

	private static short destNID;
	
	public CmdChunks() {
		super(STR_CMD, STR_UM, STR_HM);
	}
	
	// called by shell
	public boolean areParametersSane (String arguments[]) {
		if (arguments.length == 3) {
			if (arguments[2].compareTo("lastid")==0 || 
				arguments[2].compareTo("allids")==0
			   ) {
				try {
					destNID = Short.parseShort(arguments[1]);
					return true;
				}
				catch(NumberFormatException e) {}
			}
		} 		
		destNID=-1;
		printUsgae();
		return false;
	}

	// called after parameter have been checked
	public int execute(String command) {
		System.out.println("execute: command="+command);
		try {
			Core.execute(destNID, command, true);
		} catch (final DXRAMException e) {
			System.out.println("error: Core.execute failed");
		}
		return 0;
	}
}
