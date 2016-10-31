package de.hhu.bsinfo.ethnet;

/**
 * Represents a request to change the connection options
 *
 * @author Florian Klein, florian.klein@hhu.de, 18.03.2012
 */
class ChangeOperationsRequest {

    // Attributes
    private NIOConnection m_connection;
    private int m_operations;

    // Constructors

    /**
     * Creates an instance of ChangeOperationsRequest
     *
     * @param p_connection
     *         the connection
     * @param p_operations
     *         the operations
     */
    protected ChangeOperationsRequest(final NIOConnection p_connection, final int p_operations) {
        m_connection = p_connection;
        m_operations = p_operations;
    }

    // Getter

    /**
     * Returns the connection
     *
     * @return the NIOConnection
     */
    public NIOConnection getConnection() {
        return m_connection;
    }

    /**
     * Returns the operation interest
     *
     * @return the operation interest
     */
    public int getOperations() {
        return m_operations;
    }

    @Override public boolean equals(final Object p_request) {
        return this.m_connection.getDestination() == ((ChangeOperationsRequest) p_request).m_connection.getDestination() &&
                this.m_operations == ((ChangeOperationsRequest) p_request).m_operations;
    }

    @Override public int hashCode() {
        int ret = 1247687943;

        ret = 37 * ret + m_connection.getDestination();
        ret = 37 * ret + m_operations;

        return ret;
    }

    @Override public String toString() {
        return "[" + m_connection.getDestination() + ", " + m_operations + "]";
    }
}
