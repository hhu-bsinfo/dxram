package de.uniduesseldorf.dxram.commands;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
// AppID später optional abfragen

public class CmdChunkinfo extends Cmd {
				
		public String get_name()          {   return "chunkinfo";   }
		public String get_usage_message() {   return "chunkinfo NID,LID [destNID]";   }
		public String get_help_message()  {   return "Get information about chunk NID,LID (from peer NID).\nOptionally, the request can be sent to superpeer destNID. ";   }
		public String get_syntax()        {   return "chunkinfo PNID,PNR [ANID]";   }
	
	
		// called after parameter have been checked
		public int execute(String p_command) {
			String res="error";
			String arguments[];
			short NID;
			
			try {
				arguments = p_command.split(" ");
				
				// get NID to send command to
				if (arguments.length>2) 
					NID = CmdUtils.get_NID_from_string(arguments[2]);
				else
					NID = CmdUtils.get_NID_from_tuple(arguments[1]);
				
				if (CmdUtils.checkNID(Short.toString(NID)).compareTo("peer")==0) {
					//System.out.println("	   chunkinfo from peer");
					res = Core.execute_chunk_command(NID, p_command, true);
				}
				else {
					//System.out.println("	   chunkinfo from superpeer");
					res = Core.execute_lookup_command(NID, p_command, true);
				}

				// process result of remote call
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
		 
}