/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.core.util;

import java.io.ByteArrayOutputStream;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.core.impl.Notification;

public class Hex {
  public static void encNonAscii(StringBuilder sb, Bytes bytes) {
    if (bytes == null) {
      sb.append("null");
    } else {
      for (int i = 0; i < bytes.length(); i++) {
        byte b = bytes.byteAt(i);
        if (b >= 32 && b <= 126 && b != '\\') {
          sb.append((char) b);
        } else {
          sb.append(String.format("\\x%02x", b & 0xff));
        }
      }
    }
  }

  public static String encNonAscii(Bytes bytes) {
    StringBuilder sb = new StringBuilder();
    encNonAscii(sb, bytes);
    return sb.toString();
  }

  public static void encNonAscii(StringBuilder sb, Column c, String sep) {
    encNonAscii(sb, c.getFamily());
    sb.append(sep);
    encNonAscii(sb, c.getQualifier());
    sb.append(sep);
    encNonAscii(sb, c.getVisibility());

  }

  public static String encNonAscii(Column col) {
    StringBuilder sb = new StringBuilder();
    encNonAscii(sb, col, " ");
    return sb.toString();
  }

  public static String encNonAscii(RowColumn rc) {
    StringBuilder sb = new StringBuilder();
    encNonAscii(sb, rc, " ");
    return sb.toString();
  }

  public static void encNonAscii(StringBuilder sb, RowColumn rc, String sep) {
    encNonAscii(sb, rc.getRow());
    sb.append(sep);
    encNonAscii(sb, rc.getColumn(), sep);
  }

  public static String encNonAscii(Notification n) {
    StringBuilder sb = new StringBuilder();
    encNonAscii(sb, n, " ");
    sb.append(" ");
    sb.append(n.getTimestamp());
    return sb.toString();
  }

  static byte[] decode(String s) {

    // the next best thing to a StringBuilder for bytes
    ByteArrayOutputStream baos = new ByteArrayOutputStream(s.length());

    for (int i = 0; i < s.length(); i++) {
      byte b;

      if (s.charAt(i) == '\\') {
        if (s.charAt(i + 1) != 'x') {
          throw new IllegalArgumentException();
        }

        String num = "" + s.charAt(i + 2) + s.charAt(i + 3);
        b = (byte) (0xff & Integer.parseInt(num, 16));
        i += 3;
      } else {
        char c = s.charAt(i);
        if (c < 32 || c > 126) {
          throw new IllegalArgumentException();
        }

        b = (byte) (0xff & c);
      }

      baos.write(b);
    }

    return baos.toByteArray();
  }
}
