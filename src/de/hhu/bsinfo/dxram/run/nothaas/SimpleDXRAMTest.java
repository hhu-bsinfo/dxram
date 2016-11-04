package de.hhu.bsinfo.dxram.run.nothaas;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Simple test to verify if DXRAM starts and shuts down properly.
 * Run this as a peer, start one superpeer.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 18.02.2016
 */
public final class SimpleDXRAMTest extends AbstractMain {

    private DXRAM m_dxram;

    /**
     * Constructor
     */
    private SimpleDXRAMTest() {
        super("Simple test to verify if DXRAM starts and shuts down properly");

        m_dxram = new DXRAM();
        m_dxram.initialize(true);
    }

    /**
     * Java main entry point.
     *
     * @param p_args
     *     Main arguments.
     */
    public static void main(final String[] p_args) {
        AbstractMain main = new SimpleDXRAMTest();
        main.run(p_args);
    }

    @Override
    protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {

    }

    @Override
    protected int main(final ArgumentList p_arguments) {
        return 0;
    }

}
