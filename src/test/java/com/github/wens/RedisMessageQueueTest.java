package com.github.wens;

import com.github.wens.mq.MessageHandler;
import com.github.wens.mq.RedisMessageQueue;
import junit.framework.Assert;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by wens on 2017/3/7.
 */
public class RedisMessageQueueTest {


    @Test
    public void test_1() throws InterruptedException {

        final AtomicLong count = new AtomicLong(0);

        for(int i = 0 ; i < 20 ; i++){
            JedisPool jedisPool = new JedisPool("localhost" ,6379 ) ;
            RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
            redisMessageQueue.start();
            redisMessageQueue.consume("test1", new MessageHandler() {
                public void onMessage(byte[] message) {
                    System.out.println(new String( message) );
                    count.addAndGet(1);
                }
            });
        }

        final CountDownLatch countDownLatch = new CountDownLatch(5);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(5);
        for(int i = 0 ; i < 5 ; i++ ){
            new Thread(){
                @Override
                public void run() {
                    JedisPool jedisPool = new JedisPool("localhost" ,6379 ) ;
                    RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
                    try {
                        cyclicBarrier.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                    for(int j = 0 ; j < 10000 ; j++ ){
                        redisMessageQueue.publish("test1" , ("HI" + j).getBytes()  );
                    }


                    redisMessageQueue.close();
                    countDownLatch.countDown();

                }
            }.start();
        }

        countDownLatch.await();

        Thread.sleep(10000);

        Assert.assertEquals(50000l,count.get() );


    }

    @Test
    public void test_public(){
        JedisPool jedisPool = new JedisPool("localhost" ,6379 ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.publish("test1" , ("HI").getBytes()  );

    }

    @Test
    public void test_consume() throws InterruptedException {
        JedisPool jedisPool = new JedisPool("localhost" ,6379 ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        redisMessageQueue.consume("test1", new MessageHandler() {
            public void onMessage(byte[] message) {
                System.out.println(new String( message) );
            }
        });

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void test_consume2() throws InterruptedException {
        JedisPool jedisPool = new JedisPool("localhost" ,6379 ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        redisMessageQueue.consume("test1" ,"group1", new MessageHandler() {
            public void onMessage(byte[] message) {
                System.out.println(new String( message) );
            }
        });

        Thread.sleep(Long.MAX_VALUE);
    }
    @Test
    public void test_consume3() throws InterruptedException {
        JedisPool jedisPool = new JedisPool("localhost" ,6379 ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        redisMessageQueue.consume("test1" ,"group2", new MessageHandler() {
            public void onMessage(byte[] message) {
                System.out.println(new String( message) );
            }
        });

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void test_consume4() throws InterruptedException {
        JedisPool jedisPool = new JedisPool( new GenericObjectPoolConfig() , "118.89.27.94" ,12430 ,55 ,"8#tgU9UTmg" ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        redisMessageQueue.consume("course_info" ,"group1", new MessageHandler() {
            public void onMessage(byte[] message) {
                System.out.println(new String( message) );
            }
        });

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void test_public2(){
        JedisPool jedisPool = new JedisPool(new GenericObjectPoolConfig() , "localhost" ,7379, 2000 ,"xxxxxxxxxx" ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.publish("order:pay:success:mobileActivityOrder" , "{\"busId\":584981190662946816}".getBytes()  );
    }

    @Test
    public void test_consume5() throws InterruptedException {
        JedisPool jedisPool = new JedisPool( new GenericObjectPoolConfig() , "118.89.27.94" ,12430 ,2000 ,"8#tgU9UTmg" ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        redisMessageQueue.consume("test_expire" ,"group2", new MessageHandler() {
            public void onMessage(byte[] message) {
                System.out.println(new String( message) );
            }
        });

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void test_public3() throws InterruptedException {
        JedisPool jedisPool = new JedisPool( new GenericObjectPoolConfig() , "118.89.27.94" ,12430 ,5000 ,"8#tgU9UTmg" ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        while (true){
            redisMessageQueue.publish("test_expire","000000".getBytes());
            System.out.println("---");
            Thread.sleep(200 );
        }
    }

    @Test
    public void test_broadcast() throws InterruptedException {
        JedisPool jedisPool = new JedisPool( new GenericObjectPoolConfig() , "118.89.27.94" ,12430 ,5000 ,"8#tgU9UTmg" ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        int i  = 0 ;
        while (true){
            redisMessageQueue.broadcast("test",("000000" + i ).getBytes());
            System.out.println("---");
            Thread.sleep(200 );
            if(i++ > 10 ){
                break;
            }
        }
    }

    @Test
    public void test_subscribe1() throws InterruptedException {
        JedisPool jedisPool = new JedisPool( new GenericObjectPoolConfig() , "118.89.27.94" ,12430 ,2000 ,"8#tgU9UTmg" ) ;
        RedisMessageQueue redisMessageQueue = new RedisMessageQueue(jedisPool);
        redisMessageQueue.start();
        redisMessageQueue.subscribe("test" , new MessageHandler() {
            public void onMessage(byte[] message) {
                System.out.println(new String( message) );
            }
        });

        Thread.sleep(Long.MAX_VALUE);
    }






}
