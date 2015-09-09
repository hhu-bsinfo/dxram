
package de.uniduesseldorf.dxram.core.api;

/**
 * CommandListener interface (processing command network requests)
 * @author Michael Schoettner 03.09.2015
 */
public interface CommandListener {

	/**
	 * Process a command
	 * @param	p_command
	 * 				the command line string
	 * @param	p_needReply
	 * 				true: request, false: message (no reply)
	 * @return	the result string
	 */
	String processCmd(final String p_command, final boolean p_needReply);

}
