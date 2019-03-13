package de.hhu.bsinfo.dxram.job;

/**
 * Minimal job to test the JobService.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobIncTest extends Job {
    private int m_value;

    public JobIncTest() {

    }

    /**
     * Constructor.
     */
    public JobIncTest(final int p_value) {
        m_value = p_value;
    }

    /**
     * Get the test value.
     *
     * @return Value
     */
    public int getValue() {
        return m_value;
    }

    @Override
    public void execute() {
        m_value += 5;
    }
}
