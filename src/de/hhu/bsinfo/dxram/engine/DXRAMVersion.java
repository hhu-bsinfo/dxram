package de.hhu.bsinfo.dxram.engine;

import com.google.auto.value.AutoValue;

/**
 * DXRAM version object
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.2017
 */
@AutoValue
public abstract class DXRAMVersion {
    public static Builder builder() {
        return new AutoValue_DXRAMVersion.Builder();
    }

    /**
     * Get the major version number
     *
     * @return Major version number
     */
    public abstract int getMajor();

    /**
     * Get the minor version number
     *
     * @return Minor version number
     */
    public abstract int getMinor();

    /**
     * Get the revision number
     *
     * @return Revision number
     */
    public abstract int getRevision();

    @AutoValue.Builder
    public abstract static class Builder {
        /**
         * Set the major version number
         *
         * @param p_val
         *         Major version number to set
         * @return Builder
         */
        public abstract Builder setMajor(final int p_val);

        /**
         * Set the minor version number
         *
         * @param p_val
         *         Minor version number to set
         * @return Builder
         */
        public abstract Builder setMinor(final int p_val);

        /**
         * Set the revision number
         *
         * @param p_val
         *         Revision number to set
         * @return Builder
         */
        public abstract Builder setRevision(final int p_val);

        /**
         * Build the object
         *
         * @return Instance
         */
        public abstract DXRAMVersion build();
    }
}
