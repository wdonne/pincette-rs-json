package net.pincette.rs.json;

import static java.io.File.createTempFile;
import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.READ;
import static net.pincette.io.StreamConnector.copy;
import static net.pincette.json.JsonUtil.createDiff;
import static net.pincette.json.JsonUtil.createReader;
import static net.pincette.rs.Chain.with;
import static net.pincette.rs.ReadableByteChannelPublisher.readableByteChannel;
import static net.pincette.rs.Reducer.reduceJoin;
import static net.pincette.rs.json.Util.parseJson;
import static net.pincette.util.Util.tryToGetRethrow;
import static net.pincette.util.Util.tryToGetWithRethrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Flow.Processor;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import javax.json.JsonArrayBuilder;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;
import net.pincette.util.Util.GeneralException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestJson {
  private static JsonStructure arrayReducer(final Publisher<JsonValue> publisher) {
    return reduceJoin(publisher, JsonUtil::createArrayBuilder, JsonArrayBuilder::add).build();
  }

  private static boolean isSame(final File original, final JsonStructure generated) {
    return tryToGetWithRethrow(() -> read(original), JsonReader::read)
        .map(json -> createDiff(json, generated).toJsonArray().isEmpty())
        .orElse(false);
  }

  private static JsonStructure objectReducer(final Publisher<JsonValue> publisher) {
    return reduceJoin(
        with(publisher).filter(JsonUtil::isObject).map(JsonValue::asJsonObject).get(),
        JsonUtil::emptyObject,
        (i, o) -> o);
  }

  private static JsonReader read(final File json) {
    return createReader(tryToGetRethrow(() -> new FileInputStream(json)).orElse(null));
  }

  private static void test(
      final String resource,
      final Processor<ByteBuffer, JsonValue> processor,
      final int bufferSize,
      final Function<Publisher<JsonValue>, JsonStructure> reducer) {
    final File in = tryToGetRethrow(() -> createTempFile("test", "file-in.json")).orElse(null);

    try {
      copy(
          Objects.requireNonNull(TestJson.class.getResourceAsStream(resource)),
          new FileOutputStream(in));

      assertTrue(
          isSame(
              in,
              reducer.apply(
                  with(readableByteChannel(open(in.toPath(), READ), true, bufferSize))
                      .map(processor)
                      .get())));
    } catch (Exception e) {
      throw new GeneralException(e);
    } finally {
      in.delete();
    }
  }

  private static void testArray(final String resource) {
    test(resource, parseJson(), 0xffff, TestJson::arrayReducer);
    test(resource, parseJson(), 10, TestJson::arrayReducer);
  }

  private static void testObject(final String resource) {
    test(resource, parseJson(), 0xffff, TestJson::objectReducer);
    test(resource, parseJson(), 10, TestJson::objectReducer);
  }

  @Test
  @DisplayName("array1")
  void array1() {
    testArray("/array_1.json");
  }

  @Test
  @DisplayName("array2")
  void array2() {
    testArray("/array_2.json");
  }

  @Test
  @DisplayName("array values")
  void arrayValues() {
    testArray("/array_values.json");
  }

  @Test
  @DisplayName("object")
  void object() {
    testObject("/object.json");
  }
}
