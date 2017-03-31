package de.hhu.bsinfo.utils;

/**
 * Utility functions to generate random numbers
 *
 * @author Florian Klein, florian.klein@hhu.de, 05.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public class RandomUtils {

    /**
     * Utils class
     */
    private RandomUtils() {

    }

    /**
     * Creates a random value between 0 and the given upper bound
     *
     * @param p_upperBound
     *     the upper bound of the value
     * @return the created value
     */
    public static int getRandomValue(final int p_upperBound) {
        return getRandomValue(0, p_upperBound);
    }

    /**
     * Creates a random value between the given lower and upper bound
     *
     * @param p_lowerBound
     *     the lower bound of the value
     * @param p_upperBound
     *     the upper bound of the value
     * @return the created value
     */
    public static int getRandomValue(final int p_lowerBound, final int p_upperBound) {
        return (int) (Math.random() * (p_upperBound - p_lowerBound + 1)) + p_lowerBound;
    }
}
