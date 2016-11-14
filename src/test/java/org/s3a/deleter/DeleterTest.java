package org.s3a.deleter;

import org.junit.Test;

import java.io.IOException;

public class DeleterTest {

    @Test
    public void testDeleter() throws IOException {
        Deleter deleter = new Deleter("/tmp/foo");
        deleter.execute();
    }
}
