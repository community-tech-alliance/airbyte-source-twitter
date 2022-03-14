#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import logging
import requests
from airbyte_cdk.sources.streams.http import HttpStream
from requests_oauthlib import OAuth1

# Basic full refresh stream
class TwitterStream(HttpStream, ABC):
    """
    Parent class extended by all stream-specific classes
    """

    url_base = "https://api.twitter.com/2/"

    def __init__(
        self, twitter_username: str, client_key: str, client_secret: str, resource_owner_key: str, resource_owner_secret: str, **kwargs
    ):
        super().__init__(**kwargs)
        self.twitter_username = twitter_username
        self.oauth = OAuth1(
            client_key = client_key,
            client_secret = client_secret,
            resource_owner_key = resource_owner_key,
            resource_owner_secret = resource_owner_secret,
            signature_type = "auth_header",
        )
        self.max_results = 20 #results per page
        self.results_count = 0
        self.max_total_results = 10 #total results to fetch (TODO() put this into config? Maybe not a real issue)

    def _get_user_id(self):
        """
        Given a username, hit 2/users/by/username/{username} endpoint to return the numeric id of a user
        """

        user_lookup_url = f"https://api.twitter.com/2/users/by/username/{self.twitter_username}"
        r = requests.get(user_lookup_url, auth=self.oauth)
        user_data = r.json()
        user_id = user_data["data"]["id"]

        return user_id

    def _get_tweet_fields(self):
        """
        Returns a list of tweet fields to return in the response
        """

        tweet_fields = ",".join(
            [
                "id",
                "text",
                "attachments",
                "author_id",
                "context_annotations",
                "conversation_id",
                "created_at",
                "entities",
                "geo",
                "in_reply_to_user_id",
                "lang",
                "non_public_metrics",
                "organic_metrics",
                "possibly_sensitive",
                #"promoted_metrics", #This causes a null response when requested for a tweet that was not promoted; if we need this, it will need to be a separate stream
                "public_metrics",
                "referenced_tweets",
                "reply_settings",
                "source",
                "withheld",
            ]
        )

        return tweet_fields

    def _get_media_fields(self):
        """
        Returns a list of place (geo) fields to return in the response
        Media object model (API v2): https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/media
        """

        media_fields = ','.join([
            'media_key',
            'type',
            'duration_ms',
            'height',
            'non_public_metrics',
            'organic_metrics',
            'preview_image_url',
            #'promoted_metrics' #this will need its own stream if we want it; including the field for non-promoted content causes an error
            'public_metrics',
            'width',
            'alt_text'
        ])

        return media_fields

    def _get_place_fields(self):
        """
        Returns a list of place (geo) fields to return in the response
        Place object model (API v2): https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/place
        """

        place_fields = ','.join([
            'full_name',
            'id',
            'contained_within',
            'country',
            'country_code',
            'geo',
            'name',
            'place_type'
        ])

        return place_fields
    

    def get_user_metric_fields(self):
        """
        Returns a list of place (geo) fields to return in the response
        """

        user_metric_fields = ",".join([
            "id",
            "name",
            "username",
            "created_at",
            "description",
            "entities",
            "location",
            "pinned_tweet_id",
            "profile_image_url",
            "protected",
            "public_metrics",
            "url",
            "verified",
            #"withheld" #not gonna mess with this for now - https://help.twitter.com/en/rules-and-policies/tweet-withheld-by-country
        ])

        return user_metric_fields

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        response_json = response.json()

        # increment our running total of results fetched
        self.results_count += response_json.get("meta").get("result_count")
        logging.warn(f"RESULTS SO FAR: {self.results_count}")

        if response_json.get("meta").get("next_token") and self.results_count <= self.max_total_results:
            pagination_token = response_json.get("meta").get("next_token")
            return {'pagination_token': pagination_token}

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = {"max_results": self.page_size}
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:

        response_json = response.json()
        yield from response_json.get("data", [])  # Twitter puts records in a container array "data"


class Tweets(TwitterStream):
    """
    Fetch metrics for recent tweets created by the specified username.
    2/tweets endpoint: https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/api-reference/get-tweets
    Tweet object model: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    """

    primary_key = "id"
    cursor_field = "id"

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        user_id = self._get_user_id()
        tweet_fields = self._get_tweet_fields()

        endpoint = (f"users/{user_id}/tweets" +
                    f"?tweet.fields={tweet_fields}"
                    )

        return endpoint

class TweetMedia(TwitterStream):
    """
    Fetch metrics for recent tweets created by the specified username.
    2/tweets endpoint: https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/api-reference/get-tweets
    Tweet object model: https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
    """

    primary_key = "media_key"
    cursor_field = "media_key"

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:

        response_json = response.json()
        yield from response_json.get("includes", []).get("media", [])  # Twitter puts records in a container array "data"


    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        user_id = self._get_user_id()
        media_fields = self._get_media_fields()

        endpoint = (f"users/{user_id}/tweets" +
                    f"?expansions=attachments.media_keys" +
                    f"&media.fields={media_fields}"
                    )

        return endpoint


class TweetPlaces(TwitterStream):
    """
    Fetch metrics for recent tweets created by the specified username.
    2/tweets endpoint: https://developer.twitter.com/en/docs/twitter-api/tweets/lookup/api-reference/get-tweets
    Tweet object model: https://developer.twitter.com/en/docs/twitt er-api/data-dictionary/object-model/tweet
    """

    primary_key = "id"
    cursor_field = "id"

    def parse_response(
        self,
        response: requests.Response,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> Iterable[Mapping]:

        response_json = response.json()
        yield from response_json.get("includes", []).get("places", [])  # Twitter puts records in a container array "data"


    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:


        user_id = self._get_user_id()
        place_fields = self._get_place_fields()
        endpoint = (f"users/{user_id}/tweets" +
                    f"?expansions=geo.place_id" +
                    f"&place.fields={place_fields}"
                    )

        return endpoint

class UserMetrics(TwitterStream):
    """
    Returns metrics for a user identified by their username.
    """

    primary_key = "id"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return None

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs,
    ) -> str:

        user_id = self._get_user_id()
        user_metric_fields = self.get_user_metric_fields()
        endpoint = (f"users?ids={user_id}"+
                    f"&user.fields={user_metric_fields}")

        return endpoint
