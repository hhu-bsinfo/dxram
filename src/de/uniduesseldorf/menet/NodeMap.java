package de.uniduesseldorf.menet;

import java.net.InetSocketAddress;

public interface NodeMap {
	InetSocketAddress getAddress(final short p_nodeID);
}
