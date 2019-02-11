package de.hhu.bsinfo.dxram.job;

import javax.naming.Name;

import de.hhu.bsinfo.dxram.nameservice.NameserviceService;

/**
 * Minimal job to test the JobService
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class JobTest extends AbstractJob {

    public static final long TEST_CHUNK_ID = 0x4242424242424242L;
    public static final String TEST_CHUNK_NAME = "JBTST";

    public static final short MS_TYPE_ID = 1;

    private int m_value;

    public JobTest() {}

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
        NameserviceService nameService = getService(NameserviceService.class);
        nameService.register(TEST_CHUNK_ID, TEST_CHUNK_NAME);
    }
}
