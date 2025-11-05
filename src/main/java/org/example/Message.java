package org.example;

public class Message {
    public final String type;
    public final int senderId;

    public Message(String type, int senderId) {
        this.type = type;
        this.senderId = senderId;
    }

    public static Message parse(String line) {
        if (line == null) return null;
        String[] parts = line.split(":");
        if (parts.length < 2) return null;
        String type = parts[0].trim();
        int id;
        try { id = Integer.parseInt(parts[1].trim()); } catch (NumberFormatException e) { return null; }
        return new Message(type, id);
    }

    public static String format(String type, int senderId) {
        return type + ":" + senderId;
    }
}
