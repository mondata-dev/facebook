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

import com.facebook.ads.sdk.APIContext;
import com.facebook.ads.sdk.Ad;
import com.facebook.ads.sdk.AdAccount;
import com.facebook.ads.sdk.AdSet;
import com.facebook.ads.sdk.Campaign;
import com.facebook.ads.sdk.Page;
import io.cdap.plugin.facebook.source.common.SchemaHelper;
import io.cdap.plugin.facebook.source.common.config.BaseSourceConfig;
import io.cdap.plugin.facebook.source.common.config.ObjectType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;



/**
 * Creates request based on source configuration.
 */
public class InsightsRequestFactory {
  private static final Logger LOG = LoggerFactory.getLogger(InsightsRequestFactory.class);

  private static InsightsRequest createRequest(ObjectType objectType, String objectId, String accessToken) {
    APIContext context = new APIContext(accessToken).enableDebug(true);
    switch (objectType) {
      case Campaign:
        return new AdsInsightsRequestWrapper(new Campaign(objectId, context).getInsights());
      case Ad:
        return new AdsInsightsRequestWrapper(new Ad(objectId, context).getInsights());
      case AdSet:
        return new AdsInsightsRequestWrapper(new AdSet(objectId, context).getInsights());
      case Account:
        return new AdsInsightsRequestWrapper(new AdAccount(objectId, context).getInsights());
      case Page:
        return new InsightsResultRequestWrapper(new Page(objectId, context).getInsights());
      default:
        throw new IllegalArgumentException("Unsupported object");
    }
  }

  /**
   * Creates insights request.
   */
  public static InsightsRequest createRequest(BaseSourceConfig config) {
    InsightsRequest request = createRequest(config.getObjectType(), config.getObjectId(), config.getAccessToken());
    
    if (request.getClass() == AdsInsightsRequestWrapper.class) {
      List<String> fieldsToQuery = config.getFields()
        .stream()
        .filter(SchemaHelper::isValidForFieldsParameter)
        .collect(Collectors.toList());
      fieldsToQuery.forEach(request::requestField);
    } else if (request.getClass() == InsightsResultRequestWrapper.class) {
      List<String> metricsToQuery = config.getMetrics()
        .stream()
        .filter(SchemaHelper::isValidForMetricsParameter)
        .collect(Collectors.toList());
      InsightsResultRequest irr = (InsightsResultRequest) request;
      irr.requestField("name");
      irr.requestField("values");
      irr.setPeriod(config.getPeriod());
      irr.setParam("metric", metricsToQuery);
    }
   
    if (config.getFiltering() != null) {
      request.setParam("filtering", config.getFiltering());
    }

    if (!"default".equals(config.getLevel())) {
      request.setParam("level", config.getLevel());
    }

    request.setParam("date_preset", config.getDatePreset());

    return request;
  }
}
