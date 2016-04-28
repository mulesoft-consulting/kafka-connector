package org.mule.modules.kafka.automation.functional.exception;

/**
 * Created by bogdan.ilies on 25.04.2016.
 */
public class FuntionalTestException extends Throwable {

    public FuntionalTestException(String s, Throwable throwable) {
        super(s, throwable);
    }
}
