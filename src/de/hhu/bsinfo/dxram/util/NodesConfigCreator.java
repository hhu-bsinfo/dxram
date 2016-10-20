package de.hhu.bsinfo.dxram.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Small utility to create a nodes configuration file for dxram from
 * a list of role + IP elements.
 */
public class NodesConfigCreator {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Usage: <outfile> [Zookeeper IP] [Role:IP] ...");
			return;
		}

		File file = new File(args[0]);
		if (file.exists()) {
			file.delete();
		}

		try {
			file.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		PrintWriter output;
		try {
			output = new PrintWriter(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		}

		output.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+ "<conf>\n"
				+ "\t<DXRAMEngine>\n"
				+ "\t\t<ComponentSettings>\n"
				+ "\t\t\t<AbstractBootComponent>\n"
				+ "\t\t\t\t<Nodes>\n");

		for (int i = 2; i < args.length; i++) {
			String[] tokens = args[i].split(":");
			if (tokens.length == 2) {
				int port;

				if (tokens[0].equals("S")) {
					port = 22221;
				} else {
					port = 22222;
				}

				output.write("\t\t\t\t\t<Node>\n"
						+ "\t\t\t\t\t\t<IP __id=\"" + (i - 2) + "\" __type=\"str\">" + tokens[1] + "</IP>\n"
						+ "\t\t\t\t\t\t<Port __id=\"" + (i - 2) + "\" __type=\"int\">" + port + "</Port>\n"
						+ "\t\t\t\t\t\t<Role __id=\"" + (i - 2) + "\" __type=\"str\">" + tokens[0] + "</Role>\n"
						+ "\t\t\t\t\t\t<Rack __id=\"" + (i - 2) + "\" __type=\"short\">0</Rack>\n"
						+ "\t\t\t\t\t\t<Switch __id=\"" + (i - 2) + "\" __type=\"short\">0</Switch>\n"
						+ "\t\t\t\t\t</Node>\n");
			}
		}

		output.write("\t\t\t\t</Nodes>\n");

		output.write("\t\t\t\t<ZookeeperBootComponent>\n"
				+ "\t\t\t\t\t<ConnectionString __type=\"str\">" + args[1] + ":2181</ConnectionString>\n"
				+ "\t\t\t\t</ZookeeperBootComponent>\n"
				+ "\t\t\t</AbstractBootComponent>\n"
				+ "\t\t</ComponentSettings>\n"
				+ "\t</DXRAMEngine>\n"
				+ "</conf>");

		output.close();
		System.out.println("Finished writing config file to " + file.getName());
	}
}
