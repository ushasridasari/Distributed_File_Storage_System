import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple wire message exchanged between GFS components over TCP.
 * Uses a string type tag and a key-value payload map — deliberately
 * lightweight to match the reference DVCS project's plain-class style.
 */
public class Message implements Serializable {

    final String type;
    private final Map<String, Object> payload = new HashMap<>();

    public Message(String type) { this.type = type; }

    public Message put(String key, Object value) {
        payload.put(key, value);
        return this;
    }

    public Object get(String key) { return payload.get(key); }

    public boolean has(String key) { return payload.containsKey(key); }

    public static Message ok()                { return new Message("OK"); }
    public static Message error(String msg)   { return new Message("ERROR").put("error", msg); }

    @Override
    public String toString() { return "Message{type=" + type + ", payload=" + payload + "}"; }
}
