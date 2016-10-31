
package de.hhu.bsinfo.dxcompute.ms;

/**
 * Different compute roles of the master slave framework.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public enum ComputeRole {
    MASTER('M'), SLAVE('S'), NONE('N');

    public static final String MASTER_STR = "master";
    public static final String SLAVE_STR = "slave";
    public static final String NONE_STR = "none";

    private char m_acronym;

    /**
     * Creates an instance of Role
     * @param p_acronym
     *            the role's acronym
     */
    ComputeRole(final char p_acronym) {
        m_acronym = p_acronym;
    }

    /**
     * Get the node role from a full string.
     * @param p_str
     *            String to parse.
     * @return Role node of string.
     */
    public static ComputeRole toComputeRole(final String p_str) {
        String str = p_str.toLowerCase();
        switch (str) {
            case MASTER_STR:
            case "m":
                return ComputeRole.MASTER;
            case SLAVE_STR:
            case "s":
                return ComputeRole.SLAVE;
            default:
                return ComputeRole.NONE;
        }
    }

    /**
     * Gets the acronym of the role
     * @return the acronym
     */
    public char getAcronym() {
        return m_acronym;
    }

    @Override
    public String toString() {
        if (equals(MASTER)) {
            return MASTER_STR;
        } else if (equals(SLAVE)) {
            return SLAVE_STR;
        } else {
            return NONE_STR;
        }
    }

    /**
     * Gets the role for the given acronym
     * @param p_acronym
     *            the acronym
     * @return the corresponding role
     */
    public static ComputeRole getRoleByAcronym(final char p_acronym) {
        ComputeRole ret = null;

        for (ComputeRole role : values()) {
            if (role.m_acronym == p_acronym) {
                ret = role;

                break;
            }
        }

        return ret;
    }
}
