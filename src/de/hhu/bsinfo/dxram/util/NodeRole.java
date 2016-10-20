
package de.hhu.bsinfo.dxram.util;

/**
 * Represents the node roles.
 * @author Florian Klein 28.11.2013
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 9.12.15
 */
public enum NodeRole {

	// Constants
	PEER('P'), SUPERPEER('S'), TERMINAL('T');

	public static final String SUPERPEER_STR = "superpeer";
	public static final String PEER_STR = "peer";
	public static final String TERMINAL_STR = "terminal";

	// Attributes
	private char m_acronym;

	// Constructors
	/**
	 * Creates an instance of Role
	 * @param p_acronym
	 *            the role's acronym
	 */
	NodeRole(final char p_acronym) {
		m_acronym = p_acronym;
	}

	/**
	 * Get the node role from a full string.
	 * @param p_str
	 *            String to parse.
	 * @return Role node of string.
	 */
	public static NodeRole toNodeRole(final String p_str) {
		String str = p_str.toLowerCase();
		if (str.equals(SUPERPEER_STR) || str.equals("s")) {
			return NodeRole.SUPERPEER;
		} else if (str.equals(TERMINAL_STR) || str.equals("t")) {
			return NodeRole.TERMINAL;
		} else {
			return NodeRole.PEER;
		}
	}

	// Getters
	/**
	 * Gets the acronym of the role
	 * @return the acronym
	 */
	public char getAcronym() {
		return m_acronym;
	}

	@Override
	public String toString() {
		if (equals(PEER)) {
			return PEER_STR;
		} else if (equals(TERMINAL)) {
			return TERMINAL_STR;
		} else {
			return SUPERPEER_STR;
		}
	}

	// Methods
	/**
	 * Gets the role for the given acronym
	 * @param p_acronym
	 *            the acronym
	 * @return the corresponding role
	 */
	public static NodeRole getRoleByAcronym(final char p_acronym) {
		NodeRole ret = null;

		for (NodeRole role : values()) {
			if (role.m_acronym == p_acronym) {
				ret = role;

				break;
			}
		}

		return ret;
	}
}
