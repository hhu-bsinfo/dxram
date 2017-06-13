package de.hhu.bsinfo.net;

import de.hhu.bsinfo.net.core.AbstractMessage;

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
    void onIncomingMessage(AbstractMessage p_message);
}
