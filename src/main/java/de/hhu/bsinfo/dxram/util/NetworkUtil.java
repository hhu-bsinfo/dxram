package de.hhu.bsinfo.dxram.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Stream;

import org.jetbrains.annotations.Nullable;

public final class NetworkUtil {

    private NetworkUtil() {}

    /**
     * Finds the first site local address.
     *
     * @return The first site local address.
     */
    public static @Nullable InetAddress getSiteLocalAddress() {
        return getSiteLocalAddresses().findFirst().orElse(null);
    }

    /**
     * Queries all site local addresses belonging to network interfaces which are up.
     *
     * @return All site local addresses.
     */
    public static Stream<InetAddress> getSiteLocalAddresses() {
        try {
            return toStream(NetworkInterface.getNetworkInterfaces())
                    .filter(NetworkUtil::isUp)
                    .map(NetworkInterface::getInetAddresses)
                    .flatMap(NetworkUtil::toStream)
                    .filter(InetAddress::isSiteLocalAddress);
        } catch (SocketException e) {
            return Stream.empty();
        }

    }

    private static <T> Stream<T> toStream(Enumeration<T> p_enumeration) {
        return Collections.list(p_enumeration).stream();
    }

    private static boolean isUp(NetworkInterface p_networkInterface) {
        try {
            return p_networkInterface.isUp();
        } catch (SocketException e) {
            return false;
        }
    }
}
