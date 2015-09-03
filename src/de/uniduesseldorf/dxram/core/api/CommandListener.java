
package de.uniduesseldorf.dxram.core.api;

public interface CommandListener {
	public String processCmd(final String p_command, final boolean p_needReply);
}
