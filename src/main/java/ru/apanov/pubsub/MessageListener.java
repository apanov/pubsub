package ru.apanov.pubsub;

/**
 * Интерфейс для получения сообщений от продюсеров
 */
public interface MessageListener {
    void handleMessage(Message message);
}
