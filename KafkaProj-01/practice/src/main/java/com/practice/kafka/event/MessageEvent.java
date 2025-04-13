package com.practice.kafka.event;

public class MessageEvent {

    public final String key;
    public final String value;

    public MessageEvent(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
