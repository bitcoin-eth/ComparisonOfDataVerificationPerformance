package io.github.bitcoineth.distcpbenchmark.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Digests {
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private Digests() {
    }

    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    public static String toHex(byte[] bytes) {
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xFF;
            result[i * 2] = HEX[value >>> 4];
            result[i * 2 + 1] = HEX[value & 0x0F];
        }
        return new String(result);
    }
}
