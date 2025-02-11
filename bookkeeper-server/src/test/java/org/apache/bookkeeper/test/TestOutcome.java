package org.apache.bookkeeper.test;

public enum TestOutcome{
    /**When all the setup match requirements*/
    VALID,
    INCORRECT_PARAMETER_EXCEPTION,
    /** When data is null*/
    NULL,
    BK_READ_EXCEPTION,
    /**When is passed negative values or offset + length > data length*/
    ARRAY_INDEX_EXCEPTION,

    SERVER_ISSUE,
    ALREADY_EXISTS,
    INVALID_CALLBACK, CLOSED, BK_EXCEPTION, BK_EXIST;


}
