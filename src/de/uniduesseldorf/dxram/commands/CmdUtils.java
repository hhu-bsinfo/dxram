
package de.uniduesseldorf.dxram.commands;

import java.util.Iterator;
import java.util.List;

import de.uniduesseldorf.dxram.utils.ZooKeeperHandler;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler.ZooKeeperException;

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
	 * Parse NID from String
	 * @param p_str
	 *            the String
	 * @return NID
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static short getNIDfromString(final String p_str) throws NumberFormatException {
		short nodeID;

		nodeID = Short.parseShort(p_str);
		return nodeID;
	}

	/**
	 * Parse NID,LID tuple in a String and return NID
	 * @param p_str
	 *            the String
	 * @return NID
	 * @throws NumberFormatException
	 *             if an error occured
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
	 * Parse NID,LID tuple in a String and return LID
	 * @param p_str
	 *            the String
	 * @return NID
	 * @throws NumberFormatException
	 *             if an error occured
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
		localID = Integer.parseInt(chunkID[1]);
		return localID;
	}

	/**
	 * Get LID from given p_cunkID
	 * @param p_chunkID
	 *            the p_cunkID
	 * @return NID
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static long getLIDfromCID(final long p_chunkID) throws NumberFormatException {
		return p_chunkID & 0xFFFFFFFFFFFFL;
	}

	/**
	 * calc CID from given NID and LID
	 * @param p_nodeID
	 *            the NID
	 * @param p_localID
	 *            the LID
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
	 * Parse NID,LID tuple in a String and return CID
	 * @param p_str
	 *            the String
	 * @return CID
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static long getCIDfromTuple(final String p_str) throws NumberFormatException {
		final short nodeID = getNIDfromTuple(p_str);
		final long localID = getLIDfromTuple(p_str);
		return calcCID(nodeID, localID);
	}

	/**
	 * Parse CID string and return tuple string NID,LID
	 * @param p_str
	 *            the String
	 * @return NID,LID tuple
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static String getTupleFromCIDstring(final String p_str) throws NumberFormatException {
		final long chunkID = Long.parseLong(p_str);

		final int nodeID = (int) (chunkID >> 48);
		final int localID = (int) (chunkID & 0x0000FFFFFFFFFFFFL);

		final String res = nodeID + "," + localID;

		return res;
	}

	/**
	 * Convert CID to tuple string NID,LID
	 * @param p_chunkID
	 *            the cid
	 * @return NID,LID tuple
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static String getTupleFromCID(final long p_chunkID) throws NumberFormatException {

		final int nodeID = (int) (p_chunkID >> 48);
		final int localID = (int) (p_chunkID & 0x0000FFFFFFFFFFFFL);

		final String res = nodeID + "," + localID;

		return res;
	}

	/**
	 * Check if given NID is known and whether it is a superpeer or peer
	 * @param p_nodeID
	 *            the NID
	 * @return superpeer, peer, unknwon
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
	 * Check if given NID is a peer and otherwise print an error message for the command 'p_command'
	 * @param p_nodeID
	 *            the NID
	 * @param p_errorString
	 *            the error string
	 * @return true: NID is a known peer, false: unknown NID, or superpeer
	 */
	public static boolean mustBePeer(final short p_nodeID, final String p_errorString) {
		boolean ret = false;
		final String nodeIDok = checkNID(Short.toString(p_nodeID));

		if (nodeIDok.compareTo("peer") == 0) {
			ret = true;
		} else if (nodeIDok.compareTo("unknown") == 0) {
			System.out.println("error: unknown NID");
		} else {
			System.out.println("error: superpeer not allowed " + p_errorString);
		}

		return ret;
	}

	/**
	 * Check if given NID is a superpeer and otherwise print an error message for the command 'p_command'
	 * @param p_nodeID
	 *            the NID
	 * @param p_errorString
	 *            the error string
	 * @return true: NID is a known superpeer, false: unknown NID, or peer
	 */
	public static boolean mustBeSuperpeer(final short p_nodeID, final String p_errorString) {
		boolean ret = false;
		final String nodeIDok = checkNID(Short.toString(p_nodeID));

		if (nodeIDok.compareTo("superpeer") == 0) {
			ret = true;
		} else if (nodeIDok.compareTo("unknown") == 0) {
			System.out.println("error: unknown NID");
		} else {
			System.out.println("error: peer not allowed " + p_errorString);
		}

		return ret;
	}
}
