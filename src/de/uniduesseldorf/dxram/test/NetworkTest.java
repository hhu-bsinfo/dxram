
package de.uniduesseldorf.dxram.test;

import java.nio.ByteBuffer;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHandler;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHandler;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.io.InputHelper;
import de.uniduesseldorf.dxram.core.io.OutputHelper;
import de.uniduesseldorf.dxram.core.util.NodeID;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.NetworkInterface;

/**
 * Test case for the network interface
 * @author Florian Klein
 *         03.09.2013
 */
public final class NetworkTest {

	// Constants
	private static final int MESSAGE_SIZE = ConfigurationConstants.NETWORK_BUFFER_SIZE.getDefaultValue() * 3;

	// Constructors
	/**
	 * Creates an instance of NetworkTest
	 */
	private NetworkTest() {}

	/**
	 * Program entry point
	 * @param p_arguments
	 *            The program arguments
	 */
	public static void main(final String[] p_arguments) {
		Core.initialize(ConfigurationHandler.getDefaultConfiguration(), NodesConfigurationHandler.getLocalConfiguration());

		NetworkInterface network;

		try {
			network = CoreComponentFactory.getNetworkInterface();

			network.sendMessage(new TestMessage(MESSAGE_SIZE));

			while (true) {}
		} catch (final DXRAMException e) {
			e.printStackTrace();
		}
	}

	// Classes
	/**
	 * Test message
	 * @author Florian Klein
	 *         03.09.2013
	 */
	private static final class TestMessage extends AbstractMessage {

		// Attributes
		private int m_size;

		// Constructors
		/**
		 * Creates an instance of TestMessage
		 * @param p_size
		 *            the size of the message
		 */
		TestMessage(final int p_size) {
			super(NodeID.getLocalNodeID(), DEFAULT_TYPE);

			m_size = p_size;
		}

		// Methods
		@Override
		protected void writePayload(final ByteBuffer p_buffer) {
			OutputHelper.writeByteArray(p_buffer, new byte[m_size]);
		}

		@Override
		protected void readPayload(final ByteBuffer p_buffer) {
			InputHelper.readByteArray(p_buffer);
		}

		@Override
		protected int getPayloadLength() {
			return OutputHelper.getByteArrayWriteLength(m_size);
		}

	}

}
