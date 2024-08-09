package net.starschema.clouddb.jdbc;

import com.google.common.base.Splitter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/** Parsing utilities for URLs and properties. */
public final class ConnectionUtils {

  static Map<String, String> tryParseLabels(@Nullable String labels) {
    if (labels == null) {
      return Collections.emptyMap();
    }
    try {
      return Splitter.on(",").withKeyValueSeparator("=").split(labels);
    } catch (IllegalArgumentException ex) {
      return Collections.emptyMap();
    }
  }

  /**
   * Return {@code defaultValue} if {@code paramValue} is null. Otherwise, return true iff {@code
   * paramValue} is "true" (case-insensitive).
   */
  static boolean parseBooleanQueryParam(@Nullable String paramValue, boolean defaultValue) {
    return paramValue == null ? defaultValue : Boolean.parseBoolean(paramValue);
  }

  /**
   * Return null if {@code paramValue} is null. Otherwise, return an Integer iff {@code paramValue}
   * can be parsed as a positive int.
   */
  static Integer parseIntQueryParam(String param, @Nullable String paramValue)
      throws BQSQLException {
    Integer val = null;
    if (paramValue != null) {
      try {
        val = Integer.parseInt(paramValue);
        if (val < 0) {
          throw new BQSQLException(param + " must be positive.");
        }
      } catch (NumberFormatException e) {
        throw new BQSQLException("could not parse " + param + " parameter.", e);
      }
    }
    return val;
  }

  /**
   * Return an empty list if {@code string} is null. Otherwise, return an array of strings iff
   * {@code string} can be parsed as an array when split by {@code delimiter}.
   */
  static List<String> parseArrayQueryParam(@Nullable String string, Character delimiter) {
    return string == null
        ? Collections.emptyList()
        : Arrays.asList(string.split(delimiter + "\\s*"));
  }
}
