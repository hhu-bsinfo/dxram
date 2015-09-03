package de.uniduesseldorf.dxram.core.api;

public interface CommandListener {
		public String processCmd(String p_command, boolean p_needReply);
}
