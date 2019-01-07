package de.hhu.bsinfo.dxram.engine;

import org.junit.Test;

import static org.junit.Assert.*;

public class DXRAMVersionTest {

    @Test
    public void fromString() {

        final String versionStringDev = "1.2.3-dev";

        final DXRAMVersion versionDev = DXRAMVersion.fromString(versionStringDev);

        assertEquals(1, versionDev.getMajor());
        assertEquals(2, versionDev.getMinor());
        assertEquals(3, versionDev.getRevision());

        final String versionStringRel = "1.2.3";

        final DXRAMVersion versionRel = DXRAMVersion.fromString(versionStringRel);

        assertEquals(1, versionRel.getMajor());
        assertEquals(2, versionRel.getMinor());
        assertEquals(3, versionRel.getRevision());

        final String versionStringTwoDigits = "1.12.3";

        final DXRAMVersion versionTwoDigits = DXRAMVersion.fromString(versionStringTwoDigits);

        assertEquals(1, versionTwoDigits.getMajor());
        assertEquals(12, versionTwoDigits.getMinor());
        assertEquals(3, versionTwoDigits.getRevision());
    }
}