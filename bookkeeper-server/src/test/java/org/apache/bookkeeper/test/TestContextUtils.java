package org.apache.bookkeeper.test;

import lombok.Getter;

public abstract class TestContextUtils {


    @Getter
    public static class ValidCtx{
        private final String author = "Dissan Uddin Ahmed";
        private final long matricola = 0x334869;
        private final String university = "Tor Vergata";

        @Override
        public String toString() {
            return "ValidContext{" +
                    "author='" + author + '\'' +
                    ", matricola=" + matricola +
                    ", university='" + university + '\'' +
                    '}';
        }

    }

    public static class InvalidCtx{
        @Override
        public String toString() {
            throw new InvalidCtxException();
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            super.clone();
            throw new InvalidCtxException();
        }

        @Override
        public boolean equals(Object obj) {
            throw new InvalidCtxException();
        }

        @Override
        public int hashCode() {
            throw new InvalidCtxException();
        }

        public InvalidCtx() {
            super();
        }


    }
    public static class InvalidCtxException extends RuntimeException {
        public InvalidCtxException() {
            super("always wrong this instance");
        }
    }

}
