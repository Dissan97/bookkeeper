package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.api.WriteFlag;

import java.util.EnumSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteFlagsMock {


    private WriteFlagsMock() {}

    public static EnumSet<WriteFlag> getMockWriteFlags() {
        EnumSet<WriteFlag> flags = mock(EnumSet.class);
        return flags;
    }
}
