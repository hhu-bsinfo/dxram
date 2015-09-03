package de.uniduesseldorf.dxram.commands;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;


public class CmdPut extends Cmd {

	public String get_name()          {   return "put";   }
	public String get_usage_message() {   return "put NID text [strID] ";   }
	public String get_help_message()  {   return "Save data 'text' on node NID.\nOptionally, you can provide a name 'strID' to retrieve a chunk by name.\nReturns CID of created chunk in tuple format (NID,LID)";   }
	public String get_syntax()        {   return "put PNID STR [STR]";   }

	
	// called after parameter have been checked
	public int execute(String p_command) {
		String arguments[];
		
		try {
			arguments = p_command.split(" ");
			short NID = CmdUtils.get_NID_from_string(arguments[1]);
			
			String res = Core.execute_chunk_command(NID, p_command, true);

			// did we get an error message back?
			if (res.indexOf("error")>-1) {
				System.out.println(res);
				return -1;
			}
			
			// the call succeed, try to get the CID of the created chunk
			arguments=res.split(" ");
			String newCID = CmdUtils.get_tuple_from_CID_string(arguments[1]);
			
			System.out.println("  Created new chunk with CID=("+newCID+")");
			
		} catch (final DXRAMException e) {
			System.out.println("  error: Core.execute failed");
		}
		return 0;
	}
	
	public String remote_execute(String p_command) {
		Chunk c = null;
		String arguments[];

		if (p_command==null) return "  error: internal error";
		try {
			// copy data from command to ByteBuffer of chunk
			arguments=p_command.split(" ");

			// create chunk with name?
			if (arguments.length > 3) {
				c = Core.createNewChunk(p_command.length(), arguments[3]);
				if (c==null) return "  error: createNewChunk failed";
			}
			else {
				c = Core.createNewChunk(p_command.length());
				if (c==null) return "  error: createNewChunk failed";
			}
			
			ByteBuffer b = c.getData();
			b.put(arguments[2].getBytes());
			
			// now save the chunk
			Core.put(c);
			return "success: "+Long.toString(c.getChunkID());
		} catch (final DXRAMException e) {
			System.out.println("  error: Core.createNewChunk failed");
		}
		return "  error: 'put' failed";
	}
	
}
