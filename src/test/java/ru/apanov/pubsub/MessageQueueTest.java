package ru.apanov.pubsub;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class MessageQueueTest {
    private static Topic TOPIC = new Topic("test");
    private static Topic TOPIC_1 = new Topic("test1");

    @Test
    public void testConsumers() throws Exception {
        final MessageQueue queue = new MessageQueue();

        int consumerCounts = 100;

        CountDownLatch latchMessagesReceived = new CountDownLatch(consumerCounts);
        makeConsumers(queue, consumerCounts, latchMessagesReceived);

        int consumers = queue.publish(TOPIC, new Message("test_message"));
        assertEquals(consumers, consumerCounts);
        assertTrue(latchMessagesReceived.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testNoSubscribers() throws Exception {
        final MessageQueue queue = new MessageQueue();

        // в топик, на который никто не подписан сообщения не придут
        int consumers = queue.publish(TOPIC_1, new Message("test_message1"));
        assertEquals(consumers, 0);


        // убедимся, что в TOPIC_1 нет test_message1
        final CountDownLatch latch = new CountDownLatch(1);
        queue.subscribe("test_enpoint_for_topic_1", new MessageListener() {
            public void handleMessage(Message message) {
                assertEquals(message.getData(), "test_message2");
                latch.countDown();
            }
        }, TOPIC_1);

        assertEquals(1, queue.publish(TOPIC_1, new Message("test_message2")));
        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }

    @Test
    public void testMultipleProducers() throws Exception {
        final MessageQueue queue = new MessageQueue();

        int consumersCount = 100;
        int producersCount = 1000;

        CountDownLatch latchMessagesReceived = new CountDownLatch(producersCount * consumersCount);
        makeConsumers(queue, consumersCount, latchMessagesReceived);

        for (int i = 0; i < producersCount; ++i) {
            new Thread(new Runnable() {
                public void run() {
                    queue.publish(TOPIC, new Message("test_message"));
                }
            }).start();
        }

        assertTrue(latchMessagesReceived.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testUnsubscribe() throws Exception {
        final MessageQueue queue = new MessageQueue();

        String endpoint = "test_enpoint_for_topic_1";

        // ожидаем 2 сообщения
        final CountDownLatch latch = new CountDownLatch(2);
        queue.subscribe(endpoint, new MessageListener() {
            public void handleMessage(Message message) {
                assertEquals(message.getData(), "test_message2");
                latch.countDown();
            }
        }, TOPIC_1);

        assertEquals(1, queue.publish(TOPIC_1, new Message("test_message2")));

        // отписываемся от топика, на который не подписаны
        queue.unsubscribe(endpoint, TOPIC);

        assertEquals(1, queue.publish(TOPIC_1, new Message("test_message2")));

        // ждем, что все сообщения дошли
        assertTrue(latch.await(1, TimeUnit.SECONDS));


        // отписываемся от топика, на который подписаны
        queue.unsubscribe(endpoint, TOPIC_1);

        // количество консьюмеров, которые получат сообщение 0
        assertEquals(0, queue.publish(TOPIC_1, new Message("test_message2")));
    }

    private static void makeConsumers(final MessageQueue queue, int consumersCount, final CountDownLatch latchMessagesReceived) throws Exception {
        final CountDownLatch latchSubscribeAll = new CountDownLatch(consumersCount);

        for (int i = 0; i < consumersCount; ++i) {
            final String endpoint = "endpoint" + String.valueOf(i);
            new Thread(new Runnable() {
                public void run() {
                    MessageListener consumer = new MessageListener() {
                        public void handleMessage(Message message) {
                            assertEquals(message.getData(), "test_message");
                            latchMessagesReceived.countDown();
                        }
                    };
                    queue.subscribe(endpoint, consumer, TOPIC);
                    latchSubscribeAll.countDown();
                }
            }).start();
        }
        assertTrue(latchSubscribeAll.await(1, TimeUnit.SECONDS));
    }
}
