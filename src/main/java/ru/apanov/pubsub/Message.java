package ru.apanov.pubsub;

/**
 * Простое текстовое сообщение
 */
public class Message {
    private String data;
    private long timestamp;

    public Message(String data) {
        this.data = data;
        timestamp = System.currentTimeMillis();
    }

    public String getData() {
        return data;
    }

    public Message setData(String data) {
        this.data = data;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Message message = (Message) o;

        if (timestamp != message.timestamp) return false;
        return data != null ? data.equals(message.data) : message.data == null;

    }

    @Override
    public int hashCode() {
        int result = data != null ? data.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
