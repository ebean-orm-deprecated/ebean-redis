package io.ebean.redis.encode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class EncodePrefixKey implements Encode {

  private final String prefix;

  public EncodePrefixKey(String prefix) {
    this.prefix = prefix;
  }

  @Override
  public byte[] encode(Object value) {

    try {
      PrefixKey wrapped = new PrefixKey(prefix, value);
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(os);
      oos.writeObject(wrapped);

      oos.flush();
      oos.close();

      return os.toByteArray();

    } catch (IOException e) {
      throw new RuntimeException("Failed to decode cache data", e);
    }
  }

  @Override
  public Object decode(byte[] data) {
    try {
      ByteArrayInputStream is = new ByteArrayInputStream(data);
      ObjectInputStream ois = new ObjectInputStream(is);
      PrefixKey wrapped = (PrefixKey)ois.readObject();
      return wrapped.getKey();

    } catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException("Failed to decode cache data", e);
    }
  }
}
