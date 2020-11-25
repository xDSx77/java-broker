package fr.epita.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public enum Json {
    ;

    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static String encode(final Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException("exception while encoding json", jsonProcessingException);
        }
    }

    public static <RES_T> RES_T decode(final Class<RES_T> resClass,
                                       final String json) {
        try {
            return MAPPER.readValue(json, resClass);
        } catch (JsonProcessingException jsonProcessingException) {
            throw new RuntimeException("exception while decoding json", jsonProcessingException);
        }
    }
}
