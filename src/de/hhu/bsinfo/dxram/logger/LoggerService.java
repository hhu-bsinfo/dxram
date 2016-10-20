
package de.hhu.bsinfo.dxram.logger;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceManager;
import de.hhu.bsinfo.dxram.logger.messages.LoggerMessages;
import de.hhu.bsinfo.dxram.logger.messages.SetLogLevelMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.utils.logger.LogLevel;

/**
 * Service to allow the application to use the same logger as DXRAM.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 02.02.16
 */
public class LoggerService extends AbstractDXRAMService implements MessageReceiver {

	// dependent components
	private NetworkComponent m_network;
	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;

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
	public void setLogLevel(final LogLevel p_logLevel) {
		m_logger.setLogLevel(p_logLevel);
	}

	/**
	 * Set the log level for the logger.
	 *
	 * @param p_logLevel Log level string to set.
	 */
	public void setLogLevel(final String p_logLevel) {
		m_logger.setLogLevel(LogLevel.toLogLevel(p_logLevel));
	}

	/**
	 * Set the log level for the logger on another node
	 *
	 * @param p_logLevel Log level to set.
	 * @param p_nodeId   Id of the node to change the log level on
	 */
	public void setLogLevel(final LogLevel p_logLevel, final Short p_nodeId) {
		if (m_boot.getNodeID() == p_nodeId) {
			setLogLevel(p_logLevel);
		} else {
			SetLogLevelMessage message = new SetLogLevelMessage(p_nodeId, p_logLevel);
			NetworkErrorCodes err = m_network.sendMessage(message);

			// #if LOGGER >= ERROR
			if (err != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(),
						"Setting log level of node " + NodeID.toHexString(p_nodeId) + " failed: " + err);
			}
			// #endif /* LOGGER >= ERROR */
		}
	}

	/**
	 * Set the log level for the logger on another node
	 *
	 * @param p_logLevel Log level string to set.
	 * @param p_nodeId   Id of the node to change the log level on
	 */
	public void setLogLevel(final String p_logLevel, final Short p_nodeId) {
		setLogLevel(LogLevel.toLogLevel(p_logLevel), p_nodeId);
	}

	/**
	 * Log an error message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 */
	public <T> void error(final Class<T> p_class, final String p_msg) {
		m_logger.error(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log an error message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 * @param p_e     Exception to add to the log message.
	 */
	public <T> void error(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.error(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a warning message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 */
	public <T> void warn(final Class<T> p_class, final String p_msg) {
		m_logger.warn(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a warning message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 * @param p_e     Exception to add to the log message.
	 */
	public <T> void warn(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.warn(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log an info message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 */
	public <T> void info(final Class<T> p_class, final String p_msg) {
		m_logger.info(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log an info message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 * @param p_e     Exception to add to the log message.
	 */
	public <T> void info(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.info(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a debug message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 */
	public <T> void debug(final Class<T> p_class, final String p_msg) {
		m_logger.debug(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a debug message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 * @param p_e     Exception to add to the log message.
	 */
	public <T> void debug(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.debug(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a trace message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 */
	public <T> void trace(final Class<T> p_class, final String p_msg) {
		m_logger.trace(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
	}

	/**
	 * Log a trace message.
	 *
	 * @param <T>     Type of the class
	 * @param p_class Class calling this method.
	 * @param p_msg   Message to log.
	 * @param p_e     Exception to add to the log message.
	 */
	public <T> void trace(final Class<T> p_class, final String p_msg, final Exception p_e) {
		m_logger.trace(getClass(), "[" + p_class.getSimpleName() + "] " + p_msg);
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
	protected boolean startService(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		m_network = getComponent(NetworkComponent.class);
		m_boot = getComponent(AbstractBootComponent.class);
		m_logger = getComponent(LoggerComponent.class);

		m_network.registerMessageType(DXRAMMessageTypes.LOGGER_MESSAGES_TYPE,
				LoggerMessages.SUBTYPE_SET_LOG_LEVEL_MESSAGE,
				SetLogLevelMessage.class);

		m_network.register(SetLogLevelMessage.class, this);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_logger = null;

		return true;
	}

	/**
	 * Handles an incoming SetLogLevelMessage
	 *
	 * @param p_message the SetLogLevelMessage
	 */
	private void incomingSetLogLevelMessage(final SetLogLevelMessage p_message) {
		m_logger.setLogLevel(p_message.getLogLevel());
	}
}
