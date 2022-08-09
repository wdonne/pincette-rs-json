package net.pincette.rs.json;

import static javax.json.stream.JsonParser.Event.END_ARRAY;
import static javax.json.stream.JsonParser.Event.END_OBJECT;
import static javax.json.stream.JsonParser.Event.START_ARRAY;
import static javax.json.stream.JsonParser.Event.START_OBJECT;
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
 * @author Werner Donn\u00e9
 */
public class JsonValues extends ProcessorBase<Pair<Event, JsonValue>, JsonValue> {
  private final Deque<Event> stack = new LinkedList<>();
  private JsonBuilderGenerator generator;
  private long requested;

  private static boolean isStart(final Pair<Event, JsonValue> event) {
    return event.first == START_ARRAY || event.first == START_OBJECT;
  }

  public static Processor<Pair<Event, JsonValue>, JsonValue> jsonValues() {
    return new JsonValues();
  }

  @Override
  protected void emit(final long number) {
    requested += number;
    more();
  }

  private void emit(final Pair<Event, JsonValue> end) {
    write(end);
    emit(generator.build());
  }

  private void emit(final JsonValue value) {
    --requested;
    reset();
    subscriber.onNext(value);
  }

  private void endArray(final Pair<Event, JsonValue> event) {
    if (stack.pop() != START_ARRAY) {
      subscriber.onError(new JsonException("No matching START_ARRAY for END_ARRAY."));
    } else if (isEnd()) {
      emit(event);
    } else if (!stack.isEmpty()) {
      write(event);
    }
  }

  private void endObject(final Pair<Event, JsonValue> event) {
    if (stack.pop() != START_OBJECT) {
      subscriber.onError(new JsonException("No matching START_OBJECT for END_OBJECT."));
    } else if (isEnd()) {
      emit(event);
    } else if (stack.isEmpty()) {
      emit(event);
    } else {
      write(event);
    }
  }

  private boolean isEnd() {
    return stack.size() == 1 && stack.peek() == START_ARRAY;
  }

  private boolean isNewStart() {
    return (stack.size() == 1 && stack.peek() == START_OBJECT)
        || (stack.size() == 2
            && stack.peekLast() == START_ARRAY
            && (stack.peek() == START_ARRAY || stack.peek() == START_OBJECT));
  }

  private void more() {
    if (requested > 0) {
      subscription.request(1);
    }
  }

  private void newGenerator() {
    generator = new JsonBuilderGenerator();

    if (stack.peek() == START_ARRAY) {
      generator.writeStartArray();
    } else {
      generator.writeStartObject();
    }
  }

  @Override
  public void onNext(final Pair<Event, JsonValue> event) {
    if (isStart(event)) {
      start(event);
    } else if (event.first == END_ARRAY) {
      endArray(event);
    } else if (event.first == END_OBJECT) {
      endObject(event);
    } else {
      write(event);
    }

    more();
  }

  private void reset() {
    generator = null;
  }

  private void start(final Pair<Event, JsonValue> event) {
    stack.push(event.first);

    if (isNewStart()) {
      newGenerator();
    } else {
      write(event);
    }
  }

  private void write(final Pair<Event, JsonValue> event) {
    if (generator != null) {
      switch (event.first) {
        case END_ARRAY:
        case END_OBJECT:
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
        case VALUE_NUMBER:
        case VALUE_STRING:
          generator.write(event.second);
          break;
        case VALUE_TRUE:
          generator.write(true);
          break;
        default:
          break;
      }
    } else if (isEnd() && event.second != null) {
      emit(event.second);
    }
  }
}
