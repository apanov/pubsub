package ru.apanov.pubsub;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Очередь сообщений, которая позволяет отправлять и получать сообщения для многих продюсеров и консьюмеров.
 * Консьюмеры могут подписаться на топик и получать данные, которые публикуют продьюсеры.
 * Очередь безлимитная и может использовать все свободные ресурсы
 */
public class MessageQueue {
    private static final Logger logger = LoggerFactory.getLogger(MessageQueue.class);

    // У каждого консьюмера своя очередь сообщений
    private final ConcurrentHashMap<Topic, List<Channel>> topicChannels =
            new ConcurrentHashMap<Topic, List<Channel>>();

    private final ConcurrentHashMap<String, Channel> endpointChannel = new ConcurrentHashMap<String, Channel>();

    private final MessageRouter router = new MessageRouter();

    /**
     * Обработчик сообщений поступающий от продюсеров, который нужен для рассылки поступающих сообщений всем
     * подписчикам
     */
    private class Channel implements Runnable {
        final MessageListener listener;
        final BlockingQueue<Message> messages;
        final String endpoint;

        private volatile boolean run;

        Channel(MessageListener listener, String endpoint) {
            this.listener = listener;
            this.messages = new LinkedBlockingQueue<Message>();
            this.run = true;
            this.endpoint = endpoint;
        }

        void unsubscribe() {
            messages.clear();
            run = false;
        }

        void add(Message message) {
            try {
                messages.put(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupted();
                logger.info(Thread.currentThread().getName() + "was interrupted.");
            }
        }

        void start() {
            new Thread(this, "channel_for_" + endpoint).start();
        }

        public void run() {
            while (run) {
                try {
                    Message message = messages.poll(1, TimeUnit.SECONDS);
                    if (message != null) {
                        listener.handleMessage(message);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Consumer {}, was received message {}", endpoint, message);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupted();
                    logger.info(Thread.currentThread().getName() + "was interrupted.");
                }
            }
        }

        @Override
        public String toString() {
            return "Channel{" +
                    "endpoint='" + endpoint + '\'' +
                    '}';
        }
    }


    /**
     * Роутер помещающий сообщение в очереди консьюмерам, подписанным на заданный топик
     */
    private class MessageRouter {

        int routeMessage(Topic topic, Message message) {
            int i = 0;
            List<Channel> channels = topicChannels.get(topic);
            for (Channel channel: safe(channels)) {
                channel.add(message);
                i++;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Added message {} to {} channels.", message, i);
            }
            return i;
        }

        void addRoute(Channel channel, Topic topic) {
            synchronized (topicChannels) {
                List<Channel> channels = topicChannels.get(topic);
                if (channels == null) {
                    channels = new CopyOnWriteArrayList<Channel>();
                    topicChannels.put(topic, channels);
                }
                channels.add(channel);
                logger.debug("Route was added. {}, {}.", channel, topic);
            }
        }

        List<Channel> removeRoute(Channel channel, Topic topic) {
            List<Channel> channels = topicChannels.get(topic);
            if (channels != null) {
                channels.remove(channel);
            }
            logger.debug("Route was removed. {}, {}.", channel, topic);
            return channels;
        }
    }

    /**
     * @return количество консьюмеров, которые получат сообщение
     */
    int publish(Topic topic, Message message) {
        return router.routeMessage(topic, message);
    }

    void subscribe(String endpoint, MessageListener consumer, Topic topic) {
        synchronized (endpointChannel) {
            Channel channel = endpointChannel.get(endpoint);
            if (channel == null) {
                channel = new Channel(consumer, endpoint);
                endpointChannel.put(endpoint, channel);
                router.addRoute(channel, topic);
                channel.start();
            } else {
                router.addRoute(channel, topic);
            }
        }
    }

    void unsubscribe(String endpoint, Topic topic) {
        Channel channel = endpointChannel.get(endpoint);
        if (channel == null) {
            return;
        }

        List<Channel> channels = router.removeRoute(channel, topic);
        if (channels != null && channels.isEmpty()) {
            channel.unsubscribe();
            endpointChannel.remove(endpoint);
        }
    }


    private static <T> Iterable<T> safe(Iterable<T> source) {
        return source == null ? Collections.<T>emptyList() : source;
    }

}
