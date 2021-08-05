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

package io.cdap.plugin.facebook.source.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.facebook.source.common.config.Breakdowns;
import io.cdap.plugin.facebook.source.common.exceptions.IllegalInsightsFieldException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Helper class to map Facebook Insights fields sets to final {@link Schema}.
 */
public class SchemaHelper {
  /**
   * Returns selected Schema.
   * @param fields The fields
   * @param breakdowns The breakdowns
   * @return The instance of Schema
   */
  public static Schema buildAdsInsightsSchema(List<String> fields, Breakdowns breakdowns) {
    Set<String> schemaFields = Sets.newHashSet(fields);
    // ensure that fields introduced by breakdowns added to schema
    if (breakdowns != null) {
      breakdowns.getActionBreakdowns().forEach(breakdownValue -> {
        if (breakdownsWithFields.contains(breakdownValue.toString())) {
          schemaFields.add(breakdownValue.toString());
        }
      });
      breakdowns.getBreakdowns().forEach(breakdownValue -> {
        if (breakdownsWithFields.contains(breakdownValue.toString())) {
          schemaFields.add(breakdownValue.toString());
        }
      });
    }
    return Schema.recordOf("FacebookAdsInsights",
        schemaFields.stream().map(SchemaHelper::fromName).collect(Collectors.toList()));
  }
  
  public static Schema buildInsightsResultSchema(List<String> metrics) {
    return Schema.recordOf("FacebookAdsInsights",
        Arrays.asList(new Schema.Field[] {
          Schema.Field.of("date", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("metricName", Schema.of(Schema.Type.STRING)),
          Schema.Field.of("metricValue", Schema.of(Schema.Type.DOUBLE)) 
        }));
    //Set<String> schemaFields = Sets.newHashSet(metrics);
    
    //return Schema.recordOf("FacebookAdsInsights",
    //    schemaFields.stream().map(SchemaHelper::fromMetricName).collect(Collectors.toList()));
  }

  private static final Map<String, String> API_FIELD_NAME_TO_SCHEMA_NAME = ImmutableMap.<String, String>builder()
    .put("1d_click", "click_1d")
    .put("1d_view", "view_1d")
    .put("28d_click", "click_28d")
    .put("28d_view", "view_28d")
    .put("7d_click", "click_7d")
    .put("7d_view", "view_7d")
    .build();

  static Schema createAddActionStatsSchema() {
    return Schema.recordOf(
      UUID.randomUUID().toString().replace("-", ""),
      Schema.Field.of("click_1d", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("view_1d", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("click_28d", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("view_28d", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("click_7d", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("view_7d", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_canvas_component_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_canvas_component_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_carousel_card_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_carousel_card_name", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_converted_product_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_destination", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_device", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_event_channel", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_link_click_destination", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_location_code", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_reaction", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_target_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_video_asset_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_video_sound", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("action_video_type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("inline", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interactive_component_sticker_id", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("interactive_component_sticker_response", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("value", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );
  }

  /**
   * Transforms api field name to schema field name.
   * <p>
   * This is required, since some of Facebook API fields not compatible with Avro naming conventions.
   */
  static String fieldNameToSchemaName(String fieldName) {
    return API_FIELD_NAME_TO_SCHEMA_NAME.getOrDefault(fieldName, fieldName);
  }

  /**
   * Returns selected Field.
   * @param name the name
   * @return   the field of Schema
   */
  public static Schema.Field fromName(String name) {
    switch (name) {
      case "account_currency":
      case "account_id":
      case "account_name":
      case "actions_per_impression":
      case "activity_recency":
      case "ad_bid_type":
      case "ad_bid_value":
      case "ad_delivery":
      case "ad_format_asset":
      case "ad_id":
      case "ad_name":
      case "adset_bid_type":
      case "adset_bid_value":
      case "adset_budget_type":
      case "adset_budget_value":
      case "adset_delivery":
      case "adset_end":
      case "adset_id":
      case "adset_name":
      case "adset_start":
      case "age":
      case "age_targeting":
      case "app_store_clicks":
      case "attention_events_per_impression":
      case "attention_events_unq_per_reach":
      case "auction_bid":
      case "auction_competitiveness":
      case "auction_max_competitor_bid":
      case "buying_type":
      case "call_to_action_clicks":
      case "campaign_delivery":
      case "campaign_end":
      case "campaign_id":
      case "campaign_name":
      case "campaign_start":
      case "canvas_avg_view_percent":
      case "canvas_avg_view_time":
      case "card_views":
      case "clicks":
      case "cost_per_dda_countby_convs":
      case "cost_per_dwell":
      case "cost_per_dwell_3_sec":
      case "cost_per_dwell_5_sec":
      case "cost_per_dwell_7_sec":
      case "cost_per_estimated_ad_recallers":
      case "cost_per_inline_link_click":
      case "cost_per_inline_post_engagement":
      case "cost_per_total_action":
      case "cost_per_unique_click":
      case "cost_per_unique_inline_link_click":
      case "country":
      case "cpc":
      case "cpm":
      case "cpp":
      case "created_time":
      case "creative_fingerprint":
      case "ctr":
      case "date_start":
      case "date_stop":
      case "dda_countby_convs":
      case "deduping_1st_source_ratio":
      case "deduping_2nd_source_ratio":
      case "deduping_3rd_source_ratio":
      case "deduping_ratio":
      case "deeplink_clicks":
      case "device_platform":
      case "dma":
      case "dwell_3_sec":
      case "dwell_5_sec":
      case "dwell_7_sec":
      case "dwell_rate":
      case "earned_impression":
      case "estimated_ad_recall_rate":
      case "estimated_ad_recall_rate_lower_bound":
      case "estimated_ad_recall_rate_upper_bound":
      case "estimated_ad_recallers":
      case "estimated_ad_recallers_lower_bound":
      case "estimated_ad_recallers_upper_bound":
      case "frequency":
      case "frequency_value":
      case "full_view_impressions":
      case "full_view_reach":
      case "gender":
      case "gender_targeting":
      case "hourly_stats_aggregated_by_advertiser_time_zone":
      case "hourly_stats_aggregated_by_audience_time_zone":
      case "impression_device":
      case "impressions":
      case "impressions_auto_refresh":
      case "impressions_gross":
      case "inline_link_click_ctr":
      case "inline_link_clicks":
      case "inline_post_engagement":
      case "instant_experience_clicks_to_open":
      case "instant_experience_clicks_to_start":
      case "instant_experience_outbound_clicks":
      case "labels":
      case "location":
      case "newsfeed_avg_position":
      case "newsfeed_clicks":
      case "newsfeed_impressions":
      case "objective":
      case "optimization_goal":
      case "performance_indicator":
      case "place_page_id":
      case "place_page_name":
      case "placement":
      case "platform_position":
      case "product_id":
      case "publisher_platform":
      case "quality_score_ectr":
      case "quality_score_ecvr":
      case "quality_score_enfbr":
      case "quality_score_organic":
      case "reach":
      case "region":
      case "social_spend":
      case "spend":
      case "thumb_stops":
      case "today_spend":
      case "total_action_value":
      case "total_actions":
      case "total_unique_actions":
      case "unique_clicks":
      case "unique_ctr":
      case "unique_impressions":
      case "unique_inline_link_click_ctr":
      case "unique_inline_link_clicks":
      case "unique_link_clicks_ctr":
      case "updated_time":
      case "website_clicks":
      case "wish_bid":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case "actions_results":
      case "cost_per_action_result":
        return Schema.Field.of(name, Schema.nullableOf(createAddActionStatsSchema()));
      case "action_values":
      case "actions":
      case "ad_click_actions":
      case "ad_impression_actions":
      case "amount_in_catalog_currency":
      case "cancel_subscription_actions":
      case "catalog_segment_actions":
      case "catalog_segment_value_in_catalog_currency":
      case "catalog_segment_value_mobile_purchase_roas":
      case "catalog_segment_value_website_purchase_roas":
      case "conditional_time_spent_ms_over_10s_actions":
      case "conditional_time_spent_ms_over_15s_actions":
      case "conditional_time_spent_ms_over_2s_actions":
      case "conditional_time_spent_ms_over_3s_actions":
      case "conditional_time_spent_ms_over_6s_actions":
      case "contact_actions":
      case "contact_value":
      case "conversion_values":
      case "conversions":
      case "cost_per_10_sec_video_view":
      case "cost_per_15_sec_video_view":
      case "cost_per_2_sec_continuous_video_view":
      case "cost_per_action_type":
      case "cost_per_ad_click":
      case "cost_per_completed_video_view":
      case "cost_per_contact":
      case "cost_per_conversion":
      case "cost_per_customize_product":
      case "cost_per_donate":
      case "cost_per_find_location":
      case "cost_per_one_thousand_ad_impression":
      case "cost_per_outbound_click":
      case "cost_per_schedule":
      case "cost_per_start_trial":
      case "cost_per_submit_application":
      case "cost_per_subscribe":
      case "cost_per_thruplay":
      case "cost_per_unique_action_type":
      case "cost_per_unique_conversion":
      case "cost_per_unique_outbound_click":
      case "customize_product_actions":
      case "customize_product_value":
      case "donate_actions":
      case "donate_value":
      case "find_location_actions":
      case "find_location_value":
      case "interactive_component_tap":
      case "mobile_app_purchase_roas":
      case "outbound_clicks":
      case "outbound_clicks_ctr":
      case "purchase_roas":
      case "recurring_subscription_payment_actions":
      case "schedule_actions":
      case "schedule_value":
      case "start_trial_actions":
      case "start_trial_value":
      case "submit_application_actions":
      case "submit_application_value":
      case "subscribe_actions":
      case "subscribe_value":
      case "unique_actions":
      case "unique_conversions":
      case "unique_outbound_clicks":
      case "unique_outbound_clicks_ctr":
      case "unique_video_continuous_2_sec_watched_actions":
      case "unique_video_view_10_sec":
      case "unique_video_view_15_sec":
      case "video_10_sec_watched_actions":
      case "video_15_sec_watched_actions":
      case "video_30_sec_watched_actions":
      case "video_avg_time_watched_actions":
      case "video_complete_watched_actions":
      case "video_completed_view_or_15s_passed_actions":
      case "video_continuous_2_sec_watched_actions":
      case "video_p100_watched_actions":
      case "video_p25_watched_actions":
      case "video_p50_watched_actions":
      case "video_p75_watched_actions":
      case "video_play_actions":
      case "video_thruplay_watched_actions":
      case "video_time_watched_actions":
      case "website_ctr":
      case "website_purchase_roas":
        return Schema.Field.of(name, Schema.nullableOf(Schema.arrayOf(createAddActionStatsSchema())));
      default:
        throw new IllegalInsightsFieldException(name);
    }
  }

  /**
   * Returns selected Field.
   * 
   * @param name the name
   * @return the field of Schema
   */
  public static Schema.Field fromMetricName(String name) {
    switch (name) {
      case "page_impressions":
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      default:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
        //throw new IllegalInsightsFieldException(name);
    }
  }

  /**
   * Breakdowns that will directly introduce field in schema.
   */
  private static List<String> breakdownsWithFields = ImmutableList.copyOf(
    new String[]{
      "age",
      "country",
      "gender",
      "impression_device",
      "product_id",
      "region",
      "dma",
      "frequency_value",
      "hourly_stats_aggregated_by_advertiser_time_zone",
      "hourly_stats_aggregated_by_audience_time_zone",
      "place_page_id",
      "publisher_platform",
      "platform_position",
      "device_platform",
    }
  );

  private static List<String> validFieldsParamValues = ImmutableList.copyOf(
    new String[]{
      "account_currency",
      "account_id",
      "account_name",
      "action_values",
      "actions",
      "ad_id",
      "ad_name",
      "adset_id",
      "adset_name",
      "app_store_clicks",
      "buying_type",
      "campaign_id",
      "campaign_name",
      "canvas_avg_view_percent",
      "canvas_avg_view_time",
      "clicks",
      "conversion_rate_ranking",
      "conversion_values",
      "conversions",
      "cost_per_10_sec_video_view",
      "cost_per_action_type",
      "cost_per_conversion",
      "cost_per_estimated_ad_recallers",
      "cost_per_inline_link_click",
      "cost_per_inline_post_engagement",
      "cost_per_outbound_click",
      "cost_per_thruplay",
      "cost_per_unique_action_type",
      "cost_per_unique_click",
      "cost_per_unique_inline_link_click",
      "cost_per_unique_outbound_click",
      "cpc",
      "cpm",
      "cpp",
      "ctr",
      "date_start",
      "date_stop",
      "deeplink_clicks",
      "engagement_rate_ranking",
      "estimated_ad_recall_rate",
      "estimated_ad_recallers",
      "frequency",
      "full_view_impressions",
      "full_view_reach",
      "impressions",
      "inline_link_click_ctr",
      "inline_link_clicks",
      "inline_post_engagement",
      "instant_experience_clicks_to_open",
      "instant_experience_clicks_to_start",
      "instant_experience_outbound_clicks",
      "mobile_app_purchase_roas",
      "newsfeed_avg_position",
      "newsfeed_clicks",
      "newsfeed_impressions",
      "objective",
      "outbound_clicks",
      "outbound_clicks_ctr",
      "purchase_roas",
      "quality_ranking",
      "reach",
      "relevance_score",
      "social_spend",
      "spend",
      "unique_actions",
      "unique_clicks",
      "unique_ctr",
      "unique_impressions",
      "unique_inline_link_click_ctr",
      "unique_inline_link_clicks",
      "unique_link_clicks_ctr",
      "unique_outbound_clicks",
      "unique_outbound_clicks_ctr",
      "video_10_sec_watched_actions",
      "video_30_sec_watched_actions",
      "video_avg_time_watched_actions",
      "video_complete_watched_actions",
      "video_p100_watched_actions",
      "video_p25_watched_actions",
      "video_p50_watched_actions",
      "video_p75_watched_actions",
      "video_play_actions",
      "video_play_curve_actions",
      "video_thruplay_watched_actions",
      "website_clicks",
      "website_ctr",
      "website_purchase_roas"
    }
  );

  /**
   * Checks if filed name is valid to be passed to "filed" parameter.
   * <p>
   * Some of fields is generated when specific breakdown specified and can't be passed to "field" parameter.
   * This method is used to filter out such fields.
   */
  public static boolean isValidForFieldsParameter(String fieldName) {
    return validFieldsParamValues.contains(fieldName);
  }

  private static List<String> validMetricsParamValues = ImmutableList.copyOf(
      new String[] {
          "page_tab_views_login_top_unique", "page_tab_views_login_top", "page_tab_views_logout_top",
          "page_total_actions", "page_cta_clicks_logged_in_total", "page_cta_clicks_logged_in_unique",
          "page_cta_clicks_by_site_logged_in_unique", "page_cta_clicks_by_age_gender_logged_in_unique",
          "page_cta_clicks_logged_in_by_country_unique", "page_cta_clicks_logged_in_by_city_unique",
          "page_call_phone_clicks_logged_in_unique", "page_call_phone_clicks_by_age_gender_logged_in_unique",
          "page_call_phone_clicks_logged_in_by_country_unique", "page_call_phone_clicks_logged_in_by_city_unique",
          "page_call_phone_clicks_by_site_logged_in_unique", "page_get_directions_clicks_logged_in_unique",
          "page_get_directions_clicks_by_age_gender_logged_in_unique",
          "page_get_directions_clicks_logged_in_by_country_unique",
          "page_get_directions_clicks_logged_in_by_city_unique", "page_get_directions_clicks_by_site_logged_in_unique",
          "page_website_clicks_logged_in_unique", "page_website_clicks_by_age_gender_logged_in_unique",
          "page_website_clicks_logged_in_by_country_unique", "page_website_clicks_logged_in_by_city_unique",
          "page_website_clicks_by_site_logged_in_unique", "page_engaged_users", "page_post_engagements",
          "page_consumptions", "page_consumptions_unique", "page_consumptions_by_consumption_type",
          "page_consumptions_by_consumption_type_unique", "page_places_checkin_total",
          "page_places_checkin_total_unique", "page_places_checkin_mobile", "page_places_checkin_mobile_unique",
          "page_places_checkins_by_age_gender", "page_places_checkins_by_locale", "page_places_checkins_by_country",
          "page_negative_feedback", "page_negative_feedback_unique", "page_negative_feedback_by_type",
          "page_negative_feedback_by_type_unique", "page_positive_feedback_by_type",
          "page_positive_feedback_by_type_unique", "page_fans_online", "page_fans_online_per_day",
          "page_fan_adds_by_paid_non_paid_unique", "page_impressions", "page_impressions_unique",
          "page_impressions_paid", "page_impressions_paid_unique", "page_impressions_organic",
          "page_impressions_organic_unique", "page_impressions_viral", "page_impressions_viral_unique",
          "page_impressions_nonviral", "page_impressions_nonviral_unique", "page_impressions_by_story_type",
          "page_impressions_by_story_type_unique", "page_impressions_by_city_unique",
          "page_impressions_by_country_unique", "page_impressions_by_locale_unique",
          "page_impressions_by_age_gender_unique", "page_impressions_frequency_distribution",
          "page_impressions_viral_frequency_distribution", "page_posts_impressions", "page_posts_impressions_unique",
          "page_posts_impressions_paid", "page_posts_impressions_paid_unique", "page_posts_impressions_organic",
          "page_posts_impressions_organic_unique", "page_posts_served_impressions_organic_unique",
          "page_posts_impressions_viral", "page_posts_impressions_viral_unique", "page_posts_impressions_nonviral",
          "page_posts_impressions_nonviral_unique", "page_posts_impressions_frequency_distribution",
          "post_engaged_users", "post_negative_feedback", "post_negative_feedback_unique",
          "post_negative_feedback_by_type", "post_negative_feedback_by_type_unique", "post_engaged_fan", "post_clicks",
          "post_clicks_unique", "post_clicks_by_type", "post_clicks_by_type_unique", "post_impressions",
          "post_impressions_unique", "post_impressions_paid", "post_impressions_paid_unique", "post_impressions_fan",
          "post_impressions_fan_unique", "post_impressions_fan_paid", "post_impressions_fan_paid_unique",
          "post_impressions_organic", "post_impressions_organic_unique", "post_impressions_viral",
          "post_impressions_viral_unique", "post_impressions_nonviral", "post_impressions_nonviral_unique",
          "post_impressions_by_story_type", "post_impressions_by_story_type_unique", "post_reactions_like_total",
          "post_reactions_love_total", "post_reactions_wow_total", "post_reactions_haha_total",
          "post_reactions_sorry_total", "post_reactions_anger_total", "post_reactions_by_type_total",
          "page_actions_post_reactions_like_total", "page_actions_post_reactions_love_total",
          "page_actions_post_reactions_wow_total", "page_actions_post_reactions_haha_total",
          "page_actions_post_reactions_sorry_total", "page_actions_post_reactions_anger_total",
          "page_actions_post_reactions_total", "page_fans", "page_fans_locale", "page_fans_city", "page_fans_country",
          "page_fans_gender_age", "page_fan_adds", "page_fan_adds_unique", "page_fans_by_like_source",
          "page_fans_by_like_source_unique", "page_fan_removes", "page_fan_removes_unique",
          "page_fans_by_unlike_source_unique", "page_video_views", "page_video_views_paid", "page_video_views_organic",
          "page_video_views_by_paid_non_paid", "page_video_views_autoplayed", "page_video_views_click_to_play",
          "page_video_views_unique", "page_video_repeat_views", "page_video_complete_views_30s",
          "page_video_complete_views_30s_paid", "page_video_complete_views_30s_organic",
          "page_video_complete_views_30s_autoplayed", "page_video_complete_views_30s_click_to_play",
          "page_video_complete_views_30s_unique", "page_video_complete_views_30s_repeat_views",
          "post_video_complete_views_30s_autoplayed", "post_video_complete_views_30s_clicked_to_play",
          "post_video_complete_views_30s_organic", "post_video_complete_views_30s_paid",
          "post_video_complete_views_30s_unique", "page_video_views_10s", "page_video_views_10s_paid",
          "page_video_views_10s_organic", "page_video_views_10s_autoplayed", "page_video_views_10s_click_to_play",
          "page_video_views_10s_unique", "page_video_views_10s_repeat", "page_video_view_time", "page_views_total",
          "page_views_logout", "page_views_logged_in_total", "page_views_logged_in_unique",
          "page_views_external_referrals", "page_views_by_profile_tab_total",
          "page_views_by_profile_tab_logged_in_unique", "page_views_by_internal_referer_logged_in_unique",
          "page_views_by_site_logged_in_unique", "page_views_by_age_gender_logged_in_unique",
          "page_views_by_referers_logged_in_unique", "post_video_avg_time_watched", "post_video_complete_views_organic",
          "post_video_complete_views_organic_unique", "post_video_complete_views_paid",
          "post_video_complete_views_paid_unique", "post_video_retention_graph",
          "post_video_retention_graph_clicked_to_play", "post_video_retention_graph_autoplayed",
          "post_video_views_organic", "post_video_views_organic_unique", "post_video_views_paid",
          "post_video_views_paid_unique", "post_video_length", "post_video_views", "post_video_views_unique",
          "post_video_views_autoplayed", "post_video_views_clicked_to_play", "post_video_views_15s",
          "post_video_views_60s_excludes_shorter", "post_video_views_10s", "post_video_views_10s_unique",
          "post_video_views_10s_autoplayed", "post_video_views_10s_clicked_to_play", "post_video_views_10s_organic",
          "post_video_views_10s_paid", "post_video_views_10s_sound_on", "post_video_views_sound_on",
          "post_video_view_time", "post_video_view_time_organic", "post_video_view_time_by_age_bucket_and_gender",
          "post_video_view_time_by_region_id", "post_video_views_by_distribution_type",
          "post_video_view_time_by_distribution_type", "post_video_view_time_by_country_id",
          "page_content_activity_by_action_type_unique", "page_content_activity_by_age_gender_unique",
          "page_content_activity_by_city_unique", "page_content_activity_by_country_unique",
          "page_content_activity_by_locale_unique", "page_content_activity", "page_content_activity_by_action_type",
          "post_activity", "post_activity_unique", "post_activity_by_action_type",
          "post_activity_by_action_type_unique", "page_daily_video_ad_break_cpm_by_crosspost_status",
          "page_daily_video_ad_break_earnings_by_crosspost_status", "post_video_ad_break_ad_impressions",
          "post_video_ad_break_earnings", "post_video_ad_break_ad_cpm",
      }
  );
    
  /**
   * Checks if metric name is valid to be passed to "metric" parameter.
   * <p>
   */
  public static boolean isValidForMetricsParameter(String metricName) {
    System.out.println(metricName);
    return validMetricsParamValues.contains(metricName);
  }

}
