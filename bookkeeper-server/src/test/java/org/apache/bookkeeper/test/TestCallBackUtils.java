package org.apache.bookkeeper.test;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;

public abstract class TestCallBackUtils {

    public static <T> void finish(int rc, T result, CompletableFuture<? super T> future) {
        if (rc != BKException.Code.OK) {
            future.completeExceptionally(BKException.create(rc).fillInStackTrace());
        } else {
            future.complete(result);
        }
    }

    public static class ValidAddCallback extends CompletableFuture<Long> implements AsyncCallback.AddCallback {

        @Override
        public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
            finish(rc, entryId, this);
        }

        @Override
        public void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, long qwcLatency, Object ctx) {
            AsyncCallback.AddCallback.super.addCompleteWithLatency(rc, lh, entryId, qwcLatency, ctx);
        }
    }

    public static class InvalidAddCallback extends CompletableFuture<Long> implements AsyncCallback.AddCallback {

        @Override
        public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
            super.completeExceptionally(BKException.create(
                    BKException.Code.UnexpectedConditionException
            ).fillInStackTrace());
        }

        @Override
        public void addCompleteWithLatency(int rc, LedgerHandle lh, long entryId, long qwcLatency, Object ctx) {
            AsyncCallback.AddCallback.super.addCompleteWithLatency(rc, lh, entryId, qwcLatency, ctx);
        }
    }

    public static class InvalidReadCallback extends CompletableFuture<Long> implements AsyncCallback.ReadCallback {
        @Override
        public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
            finish(BKException.Code.ReadException, null, this);
        }
    }

}
