/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.facebook.source.common.requests;

import com.facebook.ads.sdk.APIException;
import com.facebook.ads.sdk.APINodeList;
import com.facebook.ads.sdk.APIRequest;
import com.facebook.ads.sdk.InsightsResult;
import com.facebook.ads.sdk.Page;

import io.cdap.plugin.facebook.source.common.config.BaseSourceConfig;

/**
 * Wraps one of the following requests: {@link Page.APIRequestGetInsights}.
 */
public class InsightsResultRequestWrapper implements InsightsResultRequest {
  private APIRequest<InsightsResult> request;

  InsightsResultRequestWrapper(APIRequest<InsightsResult> request) {
    this.request = request;
  }

  @Override
  public void requestField(String fieldName) {
    request.requestField(fieldName);
  }

  @Override
  public void setParam(String paramName, Object value) {
    request.setParam(paramName, value);
  }

  @Override
  public void setPeriod(String period) {
    request.setParam("period", period);
  }

  @Override
  public APINodeList<InsightsResult> execute() throws APIException {
    return ((Page.APIRequestGetInsights) request).execute();
  }

  @Override
  public void configure(BaseSourceConfig config) {
  }
}
