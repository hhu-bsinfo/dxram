
package de.uniduesseldorf.dxram.commands;

import java.util.Iterator;
import java.util.List;

import de.uniduesseldorf.dxram.utils.ZooKeeperHandler;
import de.uniduesseldorf.dxram.utils.ZooKeeperHandler.ZooKeeperException;

/**
 * Help methods for parsing and handling command strings
 * @author Florian Klein
 *         09.03.2012
 */

public class CmdUtils {

	/**
	 * Parse NID from String
	 * @param p_str
	 *            the String
	 * @return NID
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static short get_NID_from_string(final String p_str) throws NumberFormatException {
		short NID;

		NID = Short.parseShort(p_str);
		return NID;
	}

	/**
	 * Parse NID,LID tuple in a String and return NID
	 * @param p_str
	 *            the String
	 * @return NID
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static short get_NID_from_tuple(String p_str) throws NumberFormatException {
		short NID;

		String[] CID = p_str.split(",");
		if (CID == null) {
			throw new NumberFormatException();
		}
		NID = Short.parseShort(CID[0]);
		return NID;
	}

	/**
	 * Parse NID,LID tuple in a String and return LID
	 * @param p_str
	 *            the String
	 * @return NID
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static long get_LID_from_tuple(final String p_str) throws NumberFormatException {
		long LID;

		String[] CID = p_str.split(",");
		if (CID == null) {
			throw new NumberFormatException();
		}
		if (CID.length != 2) {
			throw new NumberFormatException();
		}
		LID = Integer.parseInt(CID[1]);
		return LID;
	}

	/**
	 * calc CID from given NID and LID
	 */
	public static long calc_CID(final short p_NID, final long p_LID) {
		long l_NID = (long) p_NID;
		long l_LID = (long) p_LID;
		long CID;

		CID = l_NID << 48;
		CID = CID + l_LID;
		return CID;
	}

	/**
	 * Parse NID,LID tuple in a String and return CID
	 * @param p_str
	 *            the String
	 * @return CID
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static long get_CID_from_tuple(final String p_str) throws NumberFormatException {
		short NID = get_NID_from_tuple(p_str);
		long LID = get_LID_from_tuple(p_str);
		return calc_CID(NID, LID);
	}

	/**
	 * Parse CID string and return tuple string NID,LID
	 * @param p_str
	 *            the String
	 * @return NID,LID tuple
	 * @throws NumberFormatException
	 *             if an error occured
	 */
	public static String get_tuple_from_CID_string(final String p_str) throws NumberFormatException {
		long CID = Long.parseLong(p_str);

		int NID = (int) (CID >> 48);
		int LID = (int) (CID & 0x0000FFFFFFFFFFFFl);

		String res = NID + "," + LID;

		return res;
	}

	/**
	 * Check if given NID is known and whether it is a superpeer or peer
	 * @param p_NID
	 *            the NID
	 * @return superpeer, peer, unknwon
	 */
	public static String checkNID(final String p_NID) {
		String res = "unknown";
		List<String> node_list;
		Iterator<String> nli;

		// search superpeers
		try {
			node_list = ZooKeeperHandler.getChildren("nodes/superpeers");
		} catch (final ZooKeeperException e) {
			System.out.println("error: could not access ZooKeeper!");
			return "error: could not access ZooKeeper!";
		}
		nli = node_list.iterator();
		while (nli.hasNext()) {
			if (nli.next().compareTo(p_NID) == 0) {
				return "superpeer";
			}
		}
		// search peers
		try {
			node_list = ZooKeeperHandler.getChildren("nodes/peers");
		} catch (final ZooKeeperException e) {
			System.out.println("error: could not access ZooKeeper!");
			return "error: could not access ZooKeeper!";
		}
		nli = node_list.iterator();
		while (nli.hasNext()) {
			if (nli.next().compareTo(p_NID) == 0) {
				return "peer";
			}
		}

		return res;
	}

	/**
	 * Check if given NID is a peer and otherwise print an error message for the command 'p_command'
	 * @param p_NID
	 *            the NID
	 * @param p_error_string
	 *            the error string
	 * @return true: NID is a known peer, false: unknown NID, or superpeer
	 */
	public static boolean mustBePeer(final short p_NID, final String p_error_string) {
		String NIDok = checkNID(Short.toString(p_NID));

		if (NIDok.compareTo("peer") == 0) {
			return true;
		} else if (NIDok.compareTo("unknown") == 0) {
			System.out.println("error: unknown NID");
		} else {
			System.out.println("error: superpeer not allowed " + p_error_string);
		}
		return false;
	}

	/**
	 * Check if given NID is a superpeer and otherwise print an error message for the command 'p_command'
	 * @param p_NID
	 *            the NID
	 * @param p_error_string
	 *            the error string
	 * @return true: NID is a known superpeer, false: unknown NID, or peer
	 */
	public static boolean mustBeSuperpeer(final short p_NID, final String p_error_string) {
		String NIDok = checkNID(Short.toString(p_NID));

		if (NIDok.compareTo("superpeer") == 0) {
			return true;
		} else if (NIDok.compareTo("unknown") == 0) {
			System.out.println("error: unknown NID");
		} else {
			System.out.println("error: peer not allowed " + p_error_string);
		}
		return false;
	}
}
