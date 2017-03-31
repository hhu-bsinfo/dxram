/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Small utility to create a nodes configuration file for dxram from
 * a list of role + IP elements.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 15.02.2016
 */
public final class NodesConfigCreator {

    /**
     * Hidden constructor
     */
    private NodesConfigCreator() {
    }

    /**
     * Main
     *
     * @param p_args
     *     arguments
     */
    public static void main(final String[] p_args) {
        boolean success = false;

        if (p_args.length < 2) {
            System.out.println("Usage: <outfile> [Zookeeper IP] [Role:IP] ...");
            return;
        }

        File file = new File(p_args[0]);
        if (file.exists()) {
            success = file.delete();
        }

        if (success) {
            try {
                success = file.createNewFile();
            } catch (final IOException e) {
                e.printStackTrace();
                return;
            }
        }

        if (success) {
            PrintWriter output;
            try {
                output = new PrintWriter(file);
            } catch (final FileNotFoundException e) {
                e.printStackTrace();
                return;
            }

            output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + "<conf>\n" + "\t<DXRAMEngine>\n" + "\t\t<ComponentSettings>\n" +
                "\t\t\t<AbstractBootComponent>\n" + "\t\t\t\t<Nodes>\n");

            for (int i = 2; i < p_args.length; i++) {
                String[] tokens = p_args[i].split(":");
                if (tokens.length == 2) {
                    int port;

                    if ("S".equals(tokens[0])) {
                        port = 22221;
                    } else {
                        port = 22222;
                    }

                    output.write("\t\t\t\t\t<Node>\n" + "\t\t\t\t\t\t<IP __id=\"" + (i - 2) + "\" __type=\"str\">" + tokens[1] + "</IP>\n" +
                        "\t\t\t\t\t\t<Port __id=\"" + (i - 2) + "\" __type=\"int\">" + port + "</Port>\n" + "\t\t\t\t\t\t<Role __id=\"" + (i - 2) +
                        "\" __type=\"str\">" + tokens[0] + "</Role>\n" + "\t\t\t\t\t\t<Rack __id=\"" + (i - 2) + "\" __type=\"short\">0</Rack>\n" +
                        "\t\t\t\t\t\t<Switch __id=\"" + (i - 2) + "\" __type=\"short\">0</Switch>\n" + "\t\t\t\t\t</Node>\n");
                }
            }

            output.write("\t\t\t\t</Nodes>\n");

            output.write("\t\t\t\t<ZookeeperBootComponent>\n" + "\t\t\t\t\t<ConnectionString __type=\"str\">" + p_args[1] + ":2181</ConnectionString>\n" +
                "\t\t\t\t</ZookeeperBootComponent>\n" + "\t\t\t</AbstractBootComponent>\n" + "\t\t</ComponentSettings>\n" + "\t</DXRAMEngine>\n" + "</conf>");

            output.close();
            System.out.println("Finished writing config file to " + file.getName());
        }
    }
}
