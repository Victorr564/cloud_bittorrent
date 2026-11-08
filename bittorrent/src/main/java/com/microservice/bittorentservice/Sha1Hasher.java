package com.microservice.bittorentservice;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Sha1Hasher {
    public static byte[] sha1(byte[] data) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        return md.digest(data);
    }

    public static String sha1Hex(byte[] data) throws NoSuchAlgorithmException {
        return toHex(sha1(data));
    }

    private static String toHex(byte[] hashBytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }
}