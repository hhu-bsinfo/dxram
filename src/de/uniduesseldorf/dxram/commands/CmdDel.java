package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;


public class CmdDel extends Cmd {
	private final static int MAX_DATA_TRANSFER = 100;

	public String get_name()          {   return "del";   }
	public String get_usage_message() {   return "del NID,LID [destNID]";   }
	public String get_help_message()  {   return "Delete chunk NID,LID.\nOptionally, the request can be sent to node destNID (must not be a superpeer).";   }
	public String get_syntax()        {   return "del PNID,PNR [PNID]";   }
	
	// called after parameter have been checked
	public int execute(String p_command) {
		String arguments[];
		short NID;
		
		try {
			arguments = p_command.split(" ");
			
			//System.out.println("del: command="+p_command);
			//System.out.println("del: arguments.length="+arguments.length);
			
			if (arguments.length<3)
				NID = CmdUtils.get_NID_from_tuple(arguments[1]);
			else 
				NID = CmdUtils.get_NID_from_string(arguments[2]);			//System.out.println("get from:"+NID);
			
			String res = Core.execute_chunk_command(NID, p_command, true);

			// did we get an error message back?
			if (res.indexOf("error")>-1) {
				System.out.println(res);
				return -1;
			}
						
			System.out.println(res);
			
		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
		}
		return 0;
	}
	
	public String remote_execute(String p_command) {
		String arguments[];
		
		if (p_command==null) return "  error: internal error";

		try {
			arguments=p_command.split(" ");

			Core.remove( CmdUtils.get_CID_from_tuple(arguments[1]) );
			
			return "  Chunk deleted.";
			
		} catch (final DXRAMException e) {
			System.out.println("Core.remove failed.");
			return "  error: 'delete' failed";
			
		}
	}
	
}
