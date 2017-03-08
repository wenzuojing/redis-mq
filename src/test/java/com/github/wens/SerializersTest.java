package com.github.wens;

import com.github.wens.mq.Serializers;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by wens on 2017/3/8.
 */
public class SerializersTest {

    static class SomeClass {
        private String name ;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test
    public void test_1(){

        SomeClass someClass = new SomeClass();
        someClass.setName("wens");

        byte[] data = Serializers.encode(someClass);

        Object obj = Serializers.decode(data);

        Assert.assertTrue(obj instanceof SomeClass );

    }
}
