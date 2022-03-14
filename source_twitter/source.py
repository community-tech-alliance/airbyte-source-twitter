#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from requests_oauthlib import OAuth1
from source_twitter.streams import (
    Tweets,
    TweetMedia,
    TweetPlaces,
    UserMetrics
)


# Source
class SourceTwitter(AbstractSource):

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        """
        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        try:

            oauth = OAuth1(
                client_key=config["consumer_key"],
                client_secret=config["consumer_secret"],
                resource_owner_key=config["access_token"],
                resource_owner_secret=config["token_secret"],
                signature_type="auth_header",
            )

            # Get the user_id associated with the twitter username in the config
            url = f"https://api.twitter.com/2/users/by/username/{config['twitter_username']}"
            r = requests.get(url, auth=oauth)
            data = r.json()
            user_id = data["data"]["id"]

            # Get recent tweets for that user_id
            tweet_fields = ",".join(
                [
                    "id",
                ]
            )
            url = f"https://api.twitter.com/2/users/{user_id}/tweets?tweet.fields={tweet_fields}"
            r = requests.get(url, auth=oauth)

            assert r.status_code == 200

            return True, None

        except Exception as e:

            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """

        oauth = OAuth1(
            client_key=config["consumer_key"],
            client_secret=config["consumer_secret"],
            resource_owner_key=config["access_token"],
            resource_owner_secret=config["token_secret"],
        )

        return [
            #TODO() make this less repetitive and dumb (I tried just passing the oauth object instead of all the stuff but then hit errors and gave up instantly)
            Tweets(
                authenticator=oauth,
                twitter_username=config["twitter_username"],
                client_key=config["consumer_key"],
                client_secret=config["consumer_secret"],
                resource_owner_key=config["access_token"],
                resource_owner_secret=config["token_secret"],
            ),
            TweetPlaces(
                authenticator=oauth,
                twitter_username=config["twitter_username"],
                client_key=config["consumer_key"],
                client_secret=config["consumer_secret"],
                resource_owner_key=config["access_token"],
                resource_owner_secret=config["token_secret"],
            ),
            TweetMedia(
                authenticator=oauth,
                twitter_username=config["twitter_username"],
                client_key=config["consumer_key"],
                client_secret=config["consumer_secret"],
                resource_owner_key=config["access_token"],
                resource_owner_secret=config["token_secret"],
            ),
            UserMetrics(
                authenticator=oauth,
                twitter_username=config["twitter_username"],
                client_key=config["consumer_key"],
                client_secret=config["consumer_secret"],
                resource_owner_key=config["access_token"],
                resource_owner_secret=config["token_secret"],
            )
        ]
