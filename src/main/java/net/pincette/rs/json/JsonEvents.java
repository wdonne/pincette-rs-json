package net.pincette.rs.json;

import static javax.json.stream.JsonParser.Event.KEY_NAME;
import static javax.json.stream.JsonParser.Event.VALUE_FALSE;
import static javax.json.stream.JsonParser.Event.VALUE_NULL;
import static javax.json.stream.JsonParser.Event.VALUE_NUMBER;
import static javax.json.stream.JsonParser.Event.VALUE_STRING;
import static javax.json.stream.JsonParser.Event.VALUE_TRUE;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDo;
import static net.pincette.util.Util.tryToGetRethrow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Flow.Processor;
import java.util.function.Supplier;
import javax.json.JsonValue;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import net.pincette.json.filter.JacksonParser;
import net.pincette.json.filter.JsonParserWrapper;
import net.pincette.rs.ProcessorBase;
import net.pincette.util.Pair;

/**
 * A reactive JSON parser that emits JSON events.
 *
 * @author Werner Donné
 */
public class JsonEvents extends ProcessorBase<ByteBuffer, Pair<Event, JsonValue>> {
  private final NonBlockingJsonParser jackson =
      (NonBlockingJsonParser)
          tryToGetRethrow(() -> new JsonFactory().createNonBlockingByteArrayParser()).orElse(null);
  private final JsonParser parser = new JsonParserWrapper(new JacksonParser(jackson));
  private final Queue<ByteBuffer> buffers = new LinkedList<>();
  private boolean completed;
  private long requested;

  private static boolean isValue(final Event event) {
    return event == VALUE_FALSE
        || event == VALUE_NULL
        || event == VALUE_NUMBER
        || event == VALUE_TRUE
        || event == VALUE_STRING;
  }

  public static Processor<ByteBuffer, Pair<Event, JsonValue>> jsonEvents() {
    return new JsonEvents();
  }

  private byte[] array(final ByteBuffer buffer) {
    final byte[] array = new byte[buffer.limit() - buffer.position()];

    buffer.get(array);

    return array;
  }

  private boolean consumeBuffer(final ByteBuffer buffer) {
    if (buffer.hasRemaining()) {
      final byte[] array = array(buffer);

      tryToDo(() -> jackson.feedInput(array, 0, array.length), subscriber::onError);

      return buffer.hasRemaining();
    }

    return false;
  }

  private void consumeBuffers() {
    while (!buffers.isEmpty() && jackson.needMoreInput()) {
      if (!consumeBuffer(buffers.peek())) {
        buffers.remove();
      }
    }
  }

  private boolean done() {
    return completed && requested == 0 && buffers.isEmpty() && !jackson.needMoreInput();
  }

  @Override
  protected void emit(final long number) {
    dispatch(
        () -> {
          requested += number;
          emit();
        });
  }

  private void emit() {
    dispatch(
        () -> {
          consumeBuffers();
          emitAvailableEvents();
          more();
          sendComplete();
        });
  }

  private void emitAvailableEvents() {
    Event event;

    while (!jackson.needMoreInput() && requested > 0 && (event = parser.next()) != null) {
      --requested;
      subscriber.onNext(pair(event, value(event)));
    }
  }

  private void more() {
    if (!completed && requested > 0 && jackson.needMoreInput()) {
      subscription.request(1);
    }
  }

  @Override
  public void onComplete() {
    dispatch(() -> completed = true);
  }

  @Override
  public void onNext(final ByteBuffer buffer) {
    dispatch(
        () -> {
          buffers.add(buffer);
          emit();
        });
  }

  private void sendComplete() {
    dispatch(
        () -> {
          if (done()) {
            subscriber.onComplete();
          }
        });
  }

  private JsonValue value(final Event event) {
    final Supplier<JsonValue> tryKey =
        () -> event == KEY_NAME ? createValue(parser.getString()) : null;

    return isValue(event) ? parser.getValue() : tryKey.get();
  }
}
