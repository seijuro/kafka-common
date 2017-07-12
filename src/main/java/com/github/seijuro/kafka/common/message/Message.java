package com.github.seijuro.kafka.common.message;

import java.util.function.Consumer;

/**
 * Created by seijuro
 */
public interface Message {
    public String toMessage();
    public void prettyPrint(Consumer<String> consumer);
}
