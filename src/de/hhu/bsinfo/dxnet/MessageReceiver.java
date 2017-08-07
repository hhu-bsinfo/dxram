package de.hhu.bsinfo.dxnet;

import de.hhu.bsinfo.dxnet.core.Message;

/**
 * Methods for reacting on incoming Messages
 *
 * @author Florian Klein, florian.klein@hhu.de, 09.03.2012
 */
public interface MessageReceiver {
    /**
     * Handles an incoming Message
     *
     * @param p_message
     *         the Message
     */
    void onIncomingMessage(Message p_message);
}
