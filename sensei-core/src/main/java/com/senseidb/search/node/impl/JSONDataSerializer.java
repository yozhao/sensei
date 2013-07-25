package com.senseidb.search.node.impl;

import java.nio.charset.Charset;

import org.json.JSONObject;

import proj.zoie.store.ZoieStoreSerializer;

import com.senseidb.conf.SenseiSchema;

public class JSONDataSerializer implements ZoieStoreSerializer<JSONObject> {

  private static Charset UTF8 = Charset.forName("UTF-8");
  private final String _uidField;

  public JSONDataSerializer(SenseiSchema schema) {
    _uidField = schema.getUidField();
  }

  @Override
  public long getUid(JSONObject data) {
    try {
      return Long.parseLong(data.optString(_uidField, "-1"));
    } catch (Exception e) {
      return -1L;
    }
  }

  @Override
  public byte[] toBytes(JSONObject data) {
    return data.toString().getBytes(UTF8);
  }
}
