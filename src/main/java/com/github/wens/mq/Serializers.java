package com.github.wens.mq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayOutputStream;

/**
 * Created by wens on 2017/3/8.
 */
public class Serializers {

    public static byte[] encode(Object obj ){
        ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
        Kryo kryo = new Kryo();
        Output output = new Output(arrayOutputStream);
        kryo.writeClassAndObject(output, obj);
        output.close();
        return arrayOutputStream.toByteArray() ;
    }

    public static Object  decode(byte[] data){
        Kryo kryo = new Kryo();
        Input input = new Input(data);
        Object obj  = kryo.readClassAndObject(input);
        input.close();
        return obj ;
    }

}
