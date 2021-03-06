/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.julianjaffe.spark_druid_connector.clients

import com.google.common.base.Throwables
import org.apache.druid.java.util.common.lifecycle.Lifecycle
import org.apache.druid.java.util.http.client.{HttpClient, HttpClientConfig, HttpClientInit}

import java.io.{Closeable, IOException}

object HttpClientHolder {
  def create: HttpClientHolder = {
    val lifecycle = new Lifecycle
    val httpClient = HttpClientInit.createClient(HttpClientConfig.builder.build, lifecycle)
    try {
      lifecycle.start()
    } catch {
      case e: Exception =>
        throw Throwables.propagate(e)
    }
    new HttpClientHolder(lifecycle, httpClient)
  }
}

class HttpClientHolder(val lifecycle: Lifecycle, val client: HttpClient) extends Closeable {
  def get: HttpClient = {
    client
  }

  @throws[IOException]
  override def close(): Unit = {
    lifecycle.stop()
  }
}
