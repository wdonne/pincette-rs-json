package net.pincette.rs.json;

import static javax.json.stream.JsonParser.Event.END_ARRAY;
import static javax.json.stream.JsonParser.Event.END_OBJECT;
import static javax.json.stream.JsonParser.Event.START_ARRAY;
import static javax.json.stream.JsonParser.Event.START_OBJECT;
import static javax.json.stream.JsonParser.Event.VALUE_FALSE;
import static javax.json.stream.JsonParser.Event.VALUE_NULL;
import static javax.json.stream.JsonParser.Event.VALUE_NUMBER;
import static javax.json.stream.JsonParser.Event.VALUE_STRING;
import static javax.json.stream.JsonParser.Event.VALUE_TRUE;
import static net.pincette.json.JsonUtil.stringValue;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.Flow.Processor;
import javax.json.JsonException;
import javax.json.JsonValue;
import javax.json.stream.JsonParser.Event;
import net.pincette.json.filter.JsonBuilderGenerator;
import net.pincette.rs.ProcessorBase;
import net.pincette.util.Pair;

/**
 * This processor builds JSON values from a stream of JSON events. If the stream is a JSON array,
 * then the values in it will be emitted. Otherwise, an individual object is emitted.
 *
 * @author Werner Donné
 */
public class JsonValues extends ProcessorBase<Pair<Event, JsonValue>, JsonValue> {
  private final Deque<Event> stack = new LinkedList<>();
  private JsonBuilderGenerator generator = new JsonBuilderGenerator();
  private long requested;

  private static boolean isStart(final Pair<Event, JsonValue> event) {
    return event.first == START_ARRAY || event.first == START_OBJECT;
  }

  private static boolean isScalar(final Event event) {
    return event == VALUE_FALSE
        || event == VALUE_TRUE
        || event == VALUE_NULL
        || event == VALUE_NUMBER
        || event == VALUE_STRING;
  }

  public static Processor<Pair<Event, JsonValue>, JsonValue> jsonValues() {
    return new JsonValues();
  }

  @Override
  protected void emit(final long number) {
    dispatch(
        () -> {
          requested += number;
          more();
        });
  }

  private void emit(final JsonValue value) {
    dispatch(
        () -> {
          --requested;
          reset();
          subscriber.onNext(value);
        });
  }

  private void endArray(final Pair<Event, JsonValue> event) {
    if (stack.pop() != START_ARRAY) {
      subscriber.onError(new JsonException("No matching START_ARRAY for END_ARRAY."));
    } else {
      if (!isOuterArrayEnd(event.first)) {
        write(event);
      }

      if (isEndInOuterArray()) {
        emit(generator.build());
      }
    }
  }

  private void endObject(final Pair<Event, JsonValue> event) {
    if (stack.pop() != START_OBJECT) {
      subscriber.onError(new JsonException("No matching START_OBJECT for END_OBJECT."));
    } else {
      write(event);

      if (isEndInOuterArray() || stack.isEmpty()) {
        emit(generator.build());
      }
    }
  }

  private boolean isEndInOuterArray() {
    return stack.size() == 1 && stack.peek() == START_ARRAY;
  }

  private boolean isOuterArrayEnd(final Event event) {
    return event == END_ARRAY && stack.isEmpty();
  }

  private boolean isOuterArrayStart(final Event event) {
    return event == START_ARRAY && stack.isEmpty();
  }

  private boolean isScalarInOuterArray(final Event event) {
    return isScalar(event) && isEndInOuterArray();
  }

  private void more() {
    if (requested > 0 && !completed() && !cancelled() && !getError()) {
      subscription.request(1);
    }
  }

  @Override
  public void onComplete() {
    dispatch(super::onComplete);
  }

  @Override
  public void onNext(final Pair<Event, JsonValue> event) {
    if (isStart(event)) {
      start(event);
    } else if (event.first == END_ARRAY) {
      endArray(event);
    } else if (event.first == END_OBJECT) {
      endObject(event);
    } else if (isScalarInOuterArray(event.first)) {
      emit(event.second);
    } else {
      write(event);
    }

    dispatch(this::more);
  }

  private void reset() {
    generator = new JsonBuilderGenerator();
  }

  private void start(final Pair<Event, JsonValue> event) {
    if (!isOuterArrayStart(event.first)) {
      write(event);
    }

    stack.push(event.first);
  }

  private void write(final Pair<Event, JsonValue> event) {
    switch (event.first) {
      case END_ARRAY, END_OBJECT:
        generator.writeEnd();
        break;
      case KEY_NAME:
        generator.writeKey(stringValue(event.second).orElse(null));
        break;
      case START_ARRAY:
        generator.writeStartArray();
        break;
      case START_OBJECT:
        generator.writeStartObject();
        break;
      case VALUE_FALSE:
        generator.write(false);
        break;
      case VALUE_NULL:
        generator.writeNull();
        break;
      case VALUE_NUMBER, VALUE_STRING:
        generator.write(event.second);
        break;
      case VALUE_TRUE:
        generator.write(true);
        break;
      default:
        break;
    }
  }
}
