/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.adatao.pa.spark;

import java.util.List;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.Column;
import com.adatao.pa.spark.DataManager.MetaInfo;


public class Utils {

	public static void printDoubleArray(String title, String fmt, double[] xs) {
		System.out.print(title + " ");
		for (double x : xs) {
			System.out.format(fmt, x);
		}
		System.out.println();
	}

  public static MetaInfo[] generateMetaInfo(Schema schema) {
    List<Column> columns = schema.getColumns();
    MetaInfo[] metaInfo = new MetaInfo[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      metaInfo[i] = new MetaInfo(columns.get(i).getName(), columns.get(i).getType().toString().toLowerCase());
    }
    return metaInfo;
  }
  
  public static String getDataContainerId(DDF ddf) {
    return ddf.getName().substring(15).replace("_", "-");
  }
}
