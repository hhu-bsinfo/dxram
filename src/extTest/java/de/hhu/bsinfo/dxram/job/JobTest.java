package de.hhu.bsinfo.dxram.job;

/**
 * Minimal job to test the JobService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobTest extends AbstractJob {
    public static final short MS_TYPE_ID = 1;

    private int m_value;

    /**
     * Constructor
     */
    public JobTest(final int p_value) {
        m_value = p_value;
    }

    /**
     * Get the test value
     *
     * @return Value
     */
    public int getValue() {
        return m_value;
    }

    @Override
    public short getTypeID() {
        return MS_TYPE_ID;
    }

    @Override
    public void execute() {
        m_value += 5;
    }
}
