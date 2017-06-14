package de.hhu.bsinfo.ethnet.core;

/**
 * Manages for reacting to connections
 *
 * @author Marc Ewert 11.04.2014
 */
public interface DataReceiver {
    /**
     * New messsage is available
     *
     * @param p_message
     *         the message which has been received
     */
    void newMessage(AbstractMessage p_message);

    void newMessages(AbstractMessage[] p_messages);

}
