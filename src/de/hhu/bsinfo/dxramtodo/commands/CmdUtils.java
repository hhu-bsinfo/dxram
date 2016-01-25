
package de.hhu.bsinfo.dxramtodo.commands;

import java.util.Iterator;
import java.util.List;

import de.hhu.bsinfo.utils.ZooKeeperHandler;
import de.hhu.bsinfo.utils.ZooKeeperHandler.ZooKeeperException;

/**
 * Help methods for parsing and handling command strings.
 * @author Michael Schoettner 03.09.2015
 */
public final class CmdUtils {

	/**
	 * Constructor
	 */
	private CmdUtils() {}

	/**
	 * Parse NodeID from String
	 * @param p_str
	 *            the String
	 * @return NodeID
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static short getNIDfromString(final String p_str) throws NumberFormatException {
		short nodeID;

		nodeID = Short.parseShort(p_str);
		return nodeID;
	}

	/**
	 * Parse NodeID,LocalID tuple in a String and return NodeID
	 * @param p_str
	 *            the String
	 * @return NodeID
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static short getNIDfromTuple(final String p_str) throws NumberFormatException {
		short nodeID;

		final String[] chunkID = p_str.split(",");
		if (chunkID == null) {
			throw new NumberFormatException();
		}
		nodeID = Short.parseShort(chunkID[0]);
		return nodeID;
	}

	/**
	 * Parse LocalID from String
	 * @param p_str
	 *            the String
	 * @return LocalID
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static long getLIDfromString(final String p_str) throws NumberFormatException {
		long localID;

		localID = Long.parseLong(p_str);
		return localID;
	}

	/**
	 * Parse NodeID,LocalID tuple in a String and return LocalID
	 * @param p_str
	 *            the String
	 * @return NodeID
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static long getLIDfromTuple(final String p_str) throws NumberFormatException {
		long localID;

		final String[] chunkID = p_str.split(",");
		if (chunkID == null) {
			throw new NumberFormatException();
		}
		if (chunkID.length != 2) {
			throw new NumberFormatException();
		}
		localID = Long.parseLong(chunkID[1]);
		return localID;
	}

	/**
	 * Get LocalID from given p_cunkID
	 * @param p_chunkID
	 *            the p_cunkID
	 * @return NodeID
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static long getLIDfromCID(final long p_chunkID) throws NumberFormatException {
		return p_chunkID & 0xFFFFFFFFFFFFL;
	}

	/**
	 * Calculate ChunkID from given NodeID and LocalID
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_localID
	 *            the LocalID
	 * @return chunkID
	 */
	public static long calcCID(final short p_nodeID, final long p_localID) {
		final long nodeID = p_nodeID;
		final long localID = p_localID;
		long chunkID;

		chunkID = nodeID << 48;
		chunkID = chunkID + localID;
		return chunkID;
	}

	/**
	 * Parse NodeID,LocalID tuple in a String and return ChunkID
	 * @param p_str
	 *            the String
	 * @return ChunkID
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static long getCIDfromTuple(final String p_str) throws NumberFormatException {
		final short nodeID = getNIDfromTuple(p_str);
		final long localID = getLIDfromTuple(p_str);
		return calcCID(nodeID, localID);
	}

	/**
	 * Parse ChunkID string and return tuple string NodeID,LocalID
	 * @param p_str
	 *            the String
	 * @return NodeID,LocalID tuple
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static String getTupleFromCIDstring(final String p_str) throws NumberFormatException {
		final long chunkID = Long.parseLong(p_str);

		final int nodeID = (int) (chunkID >> 48);
		final int localID = (int) (chunkID & 0x0000FFFFFFFFFFFFL);

		final String res = nodeID + "," + localID;

		return res;
	}

	/**
	 * Convert ChunkID to tuple string NodeID,LocalID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return NodeID,LocalID tuple
	 * @throws NumberFormatException
	 *             if an error occurred
	 */
	public static String getTupleFromCID(final long p_chunkID) throws NumberFormatException {

		final int nodeID = (int) (p_chunkID >> 48);
		final int localID = (int) (p_chunkID & 0x0000FFFFFFFFFFFFL);

		final String res = nodeID + "," + localID;

		return res;
	}

	/**
	 * Check if given NodeID is known and whether it is a superpeer or peer
	 * @param p_nodeID
	 *            the NodeID
	 * @return superpeer, peer, unknown
	 */
	public static String checkNID(final String p_nodeID) {
		String ret = null;
		List<String> nodeList = null;
		Iterator<String> nli;

		// search superpeers
		try {
			nodeList = ZooKeeperHandler.getChildren("nodes/superpeers");
		} catch (final ZooKeeperException e) {
			System.out.println("error: could not access ZooKeeper!");
			ret = "error: could not access ZooKeeper!";
		}

		if (nodeList != null) {
			nli = nodeList.iterator();
			while (nli.hasNext()) {
				if (nli.next().compareTo(p_nodeID) == 0) {
					ret = "superpeer";
					break;
				}
			}

			if (ret == null) {
				// search peers
				nodeList = null;
				try {
					nodeList = ZooKeeperHandler.getChildren("nodes/peers");
				} catch (final ZooKeeperException e) {
					System.out.println("error: could not access ZooKeeper!");
					ret = "error: could not access ZooKeeper!";
				}

				if (nodeList != null) {
					nli = nodeList.iterator();
					while (nli.hasNext()) {
						if (nli.next().compareTo(p_nodeID) == 0) {
							ret = "peer";
							break;
						}
					}
				}
			}
		}

		if (ret == null) {
			ret = "unknown";
		}

		return ret;
	}

	/**
	 * Check if given NodeID is a peer and otherwise print an error message for the command 'p_command'
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_errorString
	 *            the error string
	 * @return true: NodeID is a known peer, false: unknown NodeID, or superpeer
	 */
	public static boolean mustBePeer(final short p_nodeID, final String p_errorString) {
		boolean ret = false;
		final String nodeIDok = checkNID(Short.toString(p_nodeID));

		if (nodeIDok.compareTo("peer") == 0) {
			ret = true;
		} else if (nodeIDok.compareTo("unknown") == 0) {
			System.out.println("error: unknown NodeID");
		} else {
			System.out.println("error: superpeer not allowed " + p_errorString);
		}

		return ret;
	}

	/**
	 * Check if given NodeID is a superpeer and otherwise print an error message for the command 'p_command'
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_errorString
	 *            the error string
	 * @return true: NodeID is a known superpeer, false: unknown NodeID, or peer
	 */
	public static boolean mustBeSuperpeer(final short p_nodeID, final String p_errorString) {
		boolean ret = false;
		final String nodeIDok = checkNID(Short.toString(p_nodeID));

		if (nodeIDok.compareTo("superpeer") == 0) {
			ret = true;
		} else if (nodeIDok.compareTo("unknown") == 0) {
			System.out.println("error: unknown NodeID");
		} else {
			System.out.println("error: peer not allowed " + p_errorString);
		}

		return ret;
	}
}
