package de.hhu.bsinfo.dxutils;

import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class CRC16Test {


    @Test
    public void hash() throws Exception {

        short crc = CRC16.hash(15);

        assertThat(crc).isEqualTo((short) 0x0440);
    }
}