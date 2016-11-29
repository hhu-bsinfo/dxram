/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
