package org.apache.bookkeeper.client;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.Enumeration;
import java.util.concurrent.CompletableFuture;

public class LedgerHandleTestUtil {
        @NotNull
        @Contract(value = "_ -> new", pure = true)
        public static AsyncCallback.ReadCallback getValidReadCallback(@NotNull CompletableFuture<Enumeration<LedgerEntry>> future){
                return new SyncCallbackUtils.SyncReadCallback(future);
        }

        @NotNull
        @Contract(value = " -> new", pure = true)
        public static CompletableFuture<Enumeration<LedgerEntry>> getValidReadCompletableFuture(){
                return new CompletableFuture<>();
        }

        public static Object getValidCtx(){
                return new Object();
        }


}
