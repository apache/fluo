/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.stress.trie;

import io.fluo.api.data.Column;

import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;

/**
 * 
 */
public class Constants {
  
  public static final TypeLayer TYPEL = new TypeLayer(new StringEncoder());

  public static final Column COUNT_SEEN_COL = TYPEL.newColumn("count", "seen");
  public static final Column COUNT_WAIT_COL = TYPEL.newColumn("count", "wait");
}
