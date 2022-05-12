package ru.mai.dep806.bigdata.spark;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by EUGENEL on 02.12.2016.
 */
public class XmlUtilsTest {
    @Test
    public void splitTags() throws Exception {
        assertEquals(Arrays.asList("java", "hibernate"), XmlUtils.splitTags("&lt;java&gt;&lt;hibernate&gt;"));
        assertEquals(Arrays.asList("java", "swing", "exception", "illegalargumentexception"), XmlUtils.splitTags("&lt;java&gt;&lt;swing&gt;&lt;exception&gt;&lt;illegalargumentexception&gt;"));
        assertEquals(Collections.singletonList("java"), XmlUtils.splitTags("&lt;java&gt;"));
        assertEquals(Collections.emptyList(), XmlUtils.splitTags(""));
        assertEquals(Collections.emptyList(), XmlUtils.splitTags(null));
    }

}