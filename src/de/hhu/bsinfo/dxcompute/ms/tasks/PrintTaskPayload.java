
package de.hhu.bsinfo.dxcompute.ms.tasks;

import java.nio.charset.StandardCharsets;

import de.hhu.bsinfo.dxcompute.ms.AbstractTaskPayload;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.term.TerminalDelegate;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Print a message to the console.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public class PrintTaskPayload extends AbstractTaskPayload {

	private String m_msg = new String();

	/**
	 * Constructor
	 */
	public PrintTaskPayload() {
		super(MasterSlaveTaskPayloads.TYPE, MasterSlaveTaskPayloads.SUBTYPE_PRINT_TASK);
	}

	/**
	 * Message to print.
	 * @param p_msg
	 *            Message.
	 */
	public void setMessage(final String p_msg) {
		m_msg = p_msg;
	}

	@Override
	public int execute(final DXRAMServiceAccessor p_dxram) {
		System.out.println(m_msg);
		return 0;
	}

	@Override
	public boolean terminalCommandCallbackForParameters(final TerminalDelegate p_delegate) {
		m_msg = p_delegate.promptForUserInput("msg");

		return true;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		int size = super.exportObject(p_exporter, p_size);

		p_exporter.writeInt(m_msg.length());
		p_exporter.writeBytes(m_msg.getBytes(StandardCharsets.US_ASCII));

		return size + Integer.BYTES + m_msg.length();
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		int size = super.importObject(p_importer, p_size);

		int strLength = p_importer.readInt();
		byte[] tmp = new byte[strLength];
		p_importer.readBytes(tmp);
		m_msg = new String(tmp, StandardCharsets.US_ASCII);

		return size + Integer.BYTES + m_msg.length();
	}

	@Override
	public int sizeofObject() {
		return super.sizeofObject() + Integer.BYTES + m_msg.length();
	}
}
