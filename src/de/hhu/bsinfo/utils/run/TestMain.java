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

package de.hhu.bsinfo.utils.run;

import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.args.ArgumentList.Argument;
import de.hhu.bsinfo.utils.main.AbstractMain;

/**
 * Example/Test implementation to show usage of the main application framwork.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class TestMain extends AbstractMain {

    private static final Argument PARAM_1 = new Argument("param1", "10", true, "First parameter");
    private static final Argument PARAM_2 = new Argument("param2", "test", true, "Second parameter");
    private static final Argument PARAM_3 = new Argument("param3", "1000", true, "Third parameter");

    /**
     * Constructor
     */
    public TestMain() {
        super("Test application");
    }

    /**
     * Java main function.
     *
     * @param p_args
     *         Argument list.
     */
    public static void main(final String[] p_args) {
        AbstractMain main = new TestMain();
        main.run(p_args);
    }

    @Override
    protected void registerDefaultProgramArguments(final ArgumentList p_arguments) {
        p_arguments.setArgument(PARAM_1);
        p_arguments.setArgument(PARAM_2);
        p_arguments.setArgument(PARAM_3);
    }

    @Override
    protected int main(final ArgumentList p_arguments) {
        System.out.println(p_arguments);

        return 0;
    }
}
