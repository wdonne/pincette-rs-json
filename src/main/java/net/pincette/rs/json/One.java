package net.pincette.rs.json;

import static javax.json.stream.JsonParser.Event.END_ARRAY;
import static javax.json.stream.JsonParser.Event.END_OBJECT;
import static javax.json.stream.JsonParser.Event.START_ARRAY;
import static javax.json.stream.JsonParser.Event.START_OBJECT;

import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.Flow.Processor;
import javax.json.JsonException;
import javax.json.JsonValue;
import javax.json.stream.JsonParser.Event;
import net.pincette.rs.ProcessorBase;
import net.pincette.util.Pair;

/**
 * This processor completes when it has seen either one JSON object or one JSON array.
 *
 * @author Werner Donné
 */
public class One extends ProcessorBase<Pair<Event, JsonValue>, Pair<Event, JsonValue>> {
  private final Deque<Event> stack = new LinkedList<>();
  private boolean completed;
  private boolean done;

  public static Processor<Pair<Event, JsonValue>, Pair<Event, JsonValue>> one() {
    return new One();
  }

  @Override
  protected void emit(final long number) {
    subscription.request(number);
  }

  @Override
  public void onComplete() {
    dispatch(
        () -> {
          completed = true;

          if (done) {
            subscriber.onComplete();
          }
        });
  }

  @Override
  public void onNext(final Pair<Event, JsonValue> event) {
    dispatch(
        () -> {
          if (event.first == START_ARRAY || event.first == START_OBJECT) {
            stack.push(event.first);
          } else if (event.first == END_ARRAY) {
            if (stack.pop() != START_ARRAY) {
              subscriber.onError(new JsonException("No matching START_ARRAY for END_ARRAY."));
            }
          } else if (event.first == END_OBJECT && stack.pop() != START_OBJECT) {
            subscriber.onError(new JsonException("No matching START_OBJECT for END_OBJECT."));
          }

          subscriber.onNext(event);

          if (stack.isEmpty()) {
            done = true;

            if (completed) {
              subscriber.onComplete();
            }
          }
        });
  }
}
