
package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.logger.messages.LoggerMessages;
import de.hhu.bsinfo.dxram.logger.messages.SetLogLevelMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Service to allow the application to use the same logger as DXRAM.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.02.16
 */
public class LoggerService extends AbstractDXRAMService implements MessageReceiver {

	private static final Logger LOGGER = LogManager.getFormatterLogger(LoggerService.class.getSimpleName());

	// dependent components
	private NetworkComponent m_network;
	private AbstractBootComponent m_boot;

	/**
	 * Constructor
	 */
	public LoggerService() {
		super("logger");
	}

	/**
	 * Set the log level for the logger.
	 *
	 * @param p_logLevel Log level to set.
	 */
	public void setLogLevel(final String p_logLevel) {

		setLogLevel(Level.getLevel(p_logLevel.toUpperCase()));
	}

	/**
	 * Set the log level for the logger.
	 *
	 * @param p_logLevel Log level to set.
	 */
	public void setLogLevel(final Level p_logLevel) {

		LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
		Configuration config = ctx.getConfiguration();
		LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
		loggerConfig.setLevel(p_logLevel);
		// This causes all Loggers to refetch information from their LoggerConfig
		ctx.updateLoggers();
	}

	/**
	 * Set the log level for the logger on another node
	 *
	 * @param p_logLevel Log level to set.
	 * @param p_nodeId   Id of the node to change the log level on
	 */
	public void setLogLevel(final String p_logLevel, final Short p_nodeId) {
		setLogLevel(Level.getLevel(p_logLevel.toUpperCase()), p_nodeId);
	}

	/**
	 * Set the log level for the logger on another node
	 *
	 * @param p_logLevel Log level to set.
	 * @param p_nodeId   Id of the node to change the log level on
	 */
	public void setLogLevel(final Level p_logLevel, final Short p_nodeId) {
		if (m_boot.getNodeID() == p_nodeId) {
			setLogLevel(p_logLevel);
		} else {
			SetLogLevelMessage message = new SetLogLevelMessage(p_nodeId, p_logLevel.name());
			try {
				m_network.sendMessage(message);
			} catch (final NetworkException e) {
				// #if LOGGER >= ERROR
				LOGGER.error("Setting log level of node 0x%X failed: %s", p_nodeId, e);
				// #endif /* LOGGER >= ERROR */
			}
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.LOGGER_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE:
						incomingSetLogLevelMessage((SetLogLevelMessage) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	@Override
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		m_network = p_componentAccessor.getComponent(NetworkComponent.class);
		m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
	}

	@Override
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		m_network.registerMessageType(DXRAMMessageTypes.LOGGER_MESSAGES_TYPE,
				LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE,
				SetLogLevelMessage.class);

		m_network.register(SetLogLevelMessage.class, this);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}

	/**
	 * Handles an incoming SetLogLevelMessage
	 *
	 * @param p_message the SetLogLevelMessage
	 */
	private void incomingSetLogLevelMessage(final SetLogLevelMessage p_message) {
		setLogLevel(p_message.getLogLevel());
	}
}
