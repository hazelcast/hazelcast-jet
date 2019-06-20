package datamodel;

public class Tweet extends Event {
    private final String text;

    Tweet(int userId, long timestamp, String text) {
        super(userId, timestamp);
        this.text = text;
    }

    public String text() {
        return text;
    }
}
