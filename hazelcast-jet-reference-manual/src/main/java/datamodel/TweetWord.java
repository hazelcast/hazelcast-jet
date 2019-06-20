package datamodel;

public class TweetWord {
    private final long timestamp;
    private final String word;

    public TweetWord(long timestamp, String word) {
        this.timestamp = timestamp;
        this.word = word;
    }

    public long timestamp() {
        return timestamp;
    }

    public String word() {
        return word;
    }
}
