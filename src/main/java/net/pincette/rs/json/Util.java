package net.pincette.rs.json;

import static java.nio.ByteBuffer.wrap;
import static java.nio.charset.StandardCharsets.UTF_8;
import static net.pincette.rs.After.after;
import static net.pincette.rs.Before.before;
import static net.pincette.rs.Buffer.buffer;
import static net.pincette.rs.Filter.filter;
import static net.pincette.rs.Mapper.map;
import static net.pincette.rs.Pipe.pipe;
import static net.pincette.rs.Separator.separator;
import static net.pincette.rs.json.JsonEvents.jsonEvents;
import static net.pincette.rs.json.JsonValues.jsonValues;
import static net.pincette.rs.json.One.one;

import java.nio.ByteBuffer;
import java.util.concurrent.Flow.Processor;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.rs.Mapper;
import net.pincette.rs.Split;

/**
 * Reactive JSON utilities.
 *
 * @author Werner Donn\u00e9
 */
public class Util {
  private Util() {}

  /**
   * Parses one JSON structure from a stream. If the structure is an object, then only one value
   * will be emitted. If it is an array, then each value in the array will be emitted.
   *
   * @return The reactive streams processor.
   */
  public static Processor<ByteBuffer, JsonValue> parseJson() {
    return pipe(Split.<ByteBuffer>split())
        .then(jsonEvents())
        .then(one())
        .then(buffer(1000))
        .then(jsonValues());
  }

  private static Processor<String, ByteBuffer> write() {
    return map(s -> wrap(s.getBytes(UTF_8)));
  }

  /**
   * Writes a stream of JSON values in one array.
   *
   * @return The reactive streams processor.
   */
  public static Processor<JsonValue, ByteBuffer> writeArray() {
    return pipe(Mapper.<JsonValue, String>map(JsonUtil::string))
        .then(separator(","))
        .then(before("["))
        .then(after("]"))
        .then(write());
  }

  /**
   * Writes JSON objects as individual objects. Other JSON values are ignored.
   *
   * @return The reactive streams processor.
   */
  public static Processor<JsonValue, ByteBuffer> writeObject() {
    return pipe(filter(JsonUtil::isObject)).then(map(JsonUtil::string)).then(write());
  }
}
