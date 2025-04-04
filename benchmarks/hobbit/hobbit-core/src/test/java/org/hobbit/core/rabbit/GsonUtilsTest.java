package org.hobbit.core.rabbit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.hobbit.core.data.ErrorData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.gson.Gson;

@RunWith(Parameterized.class)
public class GsonUtilsTest {

    protected static Gson gson = new Gson();

    protected Object original;
    protected Class<?> clazz;

    public GsonUtilsTest(Object original, Class<?> clazz) {
        super();
        this.original = original;
        this.clazz = clazz;
    }

    @Test
    public void testDirect() {
        Object received = GsonUtils.deserializeObjectWithGson(gson, GsonUtils.serializeObjectWithGson(gson, original),
                ErrorData.class, false);
        Assert.assertEquals(original, received);
    }

    @Test
    public void testWithLengthPrefix() {
        byte[] data = GsonUtils.serializeObjectWithGson(gson, original);
        ByteBuffer buffer = ByteBuffer.allocate(data.length + 4);
        buffer.putInt(data.length);
        buffer.put(data);
        
        Object received = GsonUtils.deserializeObjectWithGson(gson, buffer.array(),
                ErrorData.class, true);
        Assert.assertEquals(original, received);
    }

    @Parameters
    public static List<Object[]> parameters() {
        List<Object[]> testCases = new ArrayList<>();
        ErrorData original;

        original = new ErrorData();
        original.setErrorType("http://example.org/generalError");
        original.setContainerId("123");
        testCases.add(new Object[] {original, original.getClass()});

        original = new ErrorData();
        original.setErrorType("http://example.org/generalError");
        original.setContainerId("123");
        original.setLabel("Example label");
        testCases.add(new Object[] {original, original.getClass()});

        original = new ErrorData();
        original.setErrorType("http://example.org/generalError");
        original.setContainerId("123");
        original.setLabel("Example label");
        original.setDescription("Some description");
        testCases.add(new Object[] {original, original.getClass()});

        return testCases;
    }
}
