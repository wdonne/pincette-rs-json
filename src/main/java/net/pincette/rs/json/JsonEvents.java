package net.pincette.rs.json;

import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.util.Pair.pair;
import static net.pincette.util.Util.tryToDo;
import static net.pincette.util.Util.tryToGetRethrow;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.async.NonBlockingJsonParser;
import java.nio.ByteBuffer;
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
 * @author Werner Donn\u00e9
 */
public class JsonEvents extends ProcessorBase<ByteBuffer, Pair<Event, JsonValue>> {
  private final NonBlockingJsonParser jackson =
      (NonBlockingJsonParser)
          tryToGetRethrow(() -> new JsonFactory().createNonBlockingByteArrayParser()).orElse(null);
  private final JsonParser parser = new JsonParserWrapper(new JacksonParser(jackson));
  private boolean completed;
  private long requested;

  private static boolean isValue(final Event event) {
    return event == Event.VALUE_FALSE
        || event == Event.VALUE_NULL
        || event == Event.VALUE_NUMBER
        || event == Event.VALUE_TRUE
        || event == Event.VALUE_STRING;
  }

  public static Processor<ByteBuffer, Pair<Event, JsonValue>> jsonEvents() {
    return new JsonEvents();
  }

  private byte[] array(final ByteBuffer buffer) {
    final byte[] array = new byte[buffer.limit() - buffer.position()];

    buffer.get(array);

    return array;
  }

  @Override
  protected void emit(final long number) {
    requested += number;
    emit();
  }

  private void emit() {
    if (!jackson.needMoreInput()) {
      Event event;

      while (requested > 0 && (event = parser.next()) != null) {
        --requested;
        subscriber.onNext(pair(event, value(event)));
      }
    }

    more();
  }

  private void more() {
    if (!completed && requested > 0 && jackson.needMoreInput()) {
      subscription.request(1);
    }
  }

  @Override
  public void onComplete() {
    completed = true;
    emit();
    subscriber.onComplete();
  }

  @Override
  public void onNext(final ByteBuffer buffer) {
    final byte[] array = array(buffer);

    tryToDo(
        () -> {
          jackson.feedInput(array, 0, array.length);
          emit();
        },
        subscriber::onError);
  }

  private JsonValue value(final Event event) {
    final Supplier<JsonValue> tryKey =
        () -> event == Event.KEY_NAME ? createValue(parser.getString()) : null;

    return isValue(event) ? parser.getValue() : tryKey.get();
  }
}
