
package de.uniduesseldorf.dxram.utils;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.text.DecimalFormat;
import java.util.Collections;

/**
 * Various functions
 * @author Florian Klein
 *         05.02.2014
 */
public final class Tools {

	// Constructors
	/**
	 * Creates an instance of Tools
	 */
	private Tools() {}

	// Methods
	/**
	 * Converts a size value to a readable String
	 * @param p_size
	 *            The size to convert
	 * @return the readable String
	 */
	public static String readableSize(final long p_size) {
		final String[] units = new String[] {"B", "KB", "MB", "GB", "TB"};
		int digitGroups;
		String ret;

		Contract.check(p_size >= 0, "invalid size given");

		if (p_size == 0) {
			ret = "0 B";
		} else {
			digitGroups = (int)(Math.log10(p_size) / Math.log10(1024));
			ret =
					new DecimalFormat("#,##0").format(p_size) + " bytes ("
							+ new DecimalFormat("#,##0.#").format(p_size / Math.pow(1024, digitGroups)) + " "
							+ units[digitGroups] + ")";
		}

		return ret;
	}

	/**
	 * Converts a time value to a readable String
	 * @param p_time
	 *            The time to convert
	 * @return the readable String
	 */
	public static String readableTime(final long p_time) {
		StringBuilder ret;
		long time;
		int days;
		int hours;
		int minutes;
		int seconds;
		int milliseconds;

		ret = new StringBuilder();

		time = p_time;
		milliseconds = (int)(time % 1000);

		time = time / 1000;
		seconds = (int)(time % 60);

		time = time / 60;
		minutes = (int)(time % 60);

		time = time / 60;
		hours = (int)(time % 24);

		time = time / 24;
		days = (int)time;

		if (days > 0) {
			ret.append(days + " days ");
		}
		if (hours > 0) {
			ret.append(hours + " hours ");
		}
		if (minutes > 0) {
			ret.append(minutes + " minutes ");
		}
		if (seconds > 0) {
			ret.append(seconds + " seconds ");
		}
		ret.append(milliseconds + " milliseconds");

		return ret.toString();
	}

	/**
	 * Converts a time value to a readable String
	 * @param p_time
	 *            The time to convert
	 * @return the readable String
	 */
	public static String readableNanoTime(final long p_time) {
		StringBuilder ret;
		long time;
		int days;
		int hours;
		int minutes;
		int seconds;
		int milliseconds;
		int microseconds;
		int nanoseconds;

		ret = new StringBuilder();

		time = p_time;
		nanoseconds = (int)(time % 1000);

		time = time / 1000;
		microseconds = (int)(time % 1000);

		time = time / 1000;
		milliseconds = (int)(time % 1000);

		time = time / 1000;
		seconds = (int)(time % 60);

		time = time / 60;
		minutes = (int)(time % 60);

		time = time / 60;
		hours = (int)(time % 24);

		time = time / 24;
		days = (int)time;

		if (days > 0) {
			ret.append(days + " days ");
		}
		if (hours > 0) {
			ret.append(hours + " hours ");
		}
		if (minutes > 0) {
			ret.append(minutes + " minutes ");
		}
		if (seconds > 0) {
			ret.append(seconds + " seconds ");
		}
		if (milliseconds > 0) {
			ret.append(milliseconds + " milliseconds ");
		}
		if (microseconds > 0) {
			ret.append(microseconds + " microseconds ");
		}
		ret.append(nanoseconds + " nanoseconds");

		return ret.toString();
	}

	/**
	 * Determines the local IP address
	 * @return the local IP address
	 */
	public static String getLocalIP() {
		String ret = "127.0.0.1";

		try {
			for (NetworkInterface nif : Collections.list(NetworkInterface.getNetworkInterfaces())) {
				if (nif.isUp() && !nif.isLoopback() && !nif.isVirtual()) {
					for (InetAddress address : Collections.list(nif.getInetAddresses())) {
						if (address instanceof Inet4Address) {
							ret = address.getHostAddress();
						}
					}
				}
			}
		} catch (final SocketException e) {}

		return ret;
	}

	/**
	 * Determines a free port
	 * @param p_startPort
	 *            the port from where the scan starts
	 * @return the free port, or -1 if no free port is found
	 */
	public static int getFreePort(final int p_startPort) {
		int ret = -1;
		ServerSocket socket;

		Contract.check(p_startPort >= 0, "invalid port given");

		for (int i = p_startPort;i < 65536;i++) {
			try {
				socket = new ServerSocket(i);
				socket.close();

				ret = i;
				break;
			} catch (final IOException e) {}
		}

		return ret;
	}

	/**
	 * Creates a random value between 0 and the given upper bound
	 * @param p_upperBound
	 *            the upper bound of the value
	 * @return the created value
	 */
	public static int getRandomValue(final int p_upperBound) {
		return getRandomValue(0, p_upperBound);
	}

	/**
	 * Creates a random value between the given lower and upper bound
	 * @param p_lowerBound
	 *            the lower bound of the value
	 * @param p_upperBound
	 *            the upper bound of the value
	 * @return the created value
	 */
	public static int getRandomValue(final int p_lowerBound, final int p_upperBound) {
		return (int)(Math.random() * (p_upperBound - p_lowerBound + 1)) + p_lowerBound;
	}

}
