/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxnet.core;

/**
 * This is a default message which is never processed on the receiver.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 06.10.2016
 */
public class DefaultMessage extends Message {

    /**
     * Creates an instance of DefaultMessage.
     */
    public DefaultMessage() {
        super();
    }

    /**
     * Creates an instance of DefaultMessage
     *
     * @param p_destination
     *         the destination nodeID
     */
    public DefaultMessage(final short p_destination) {
        super(p_destination, Messages.NETWORK_MESSAGES_TYPE, Messages.SUBTYPE_DEFAULT_MESSAGE);
    }

}
