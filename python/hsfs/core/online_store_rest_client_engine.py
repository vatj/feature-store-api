#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
from typing import Optional, Any, Union, Dict, List
from datetime import datetime, timezone

from hsfs.core import online_store_rest_client_api
from hsfs import util
import hsfs

import logging
import json

_logger = logging.getLogger(__name__)


class OnlineStoreRestClientEngine:
    RETURN_TYPE_FEATURE_VALUE_DICT = "feature_value_dict"
    RETURN_TYPE_RESPONSE_JSON = "response_json"  # as a python dict
    SQL_TIMESTAMP_STRING_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self,
        features: List["hsfs.training_dataset_feature.TrainingDatasetFeature"],
        skip_fg_ids: List[int],
    ) -> "OnlineStoreRestClientEngine":
        """Initialize the RonDB Rest Server Feature Store API client.

        # Arguments:
            features: A list of features to be used for the feature vector conversion.
            skip_fg_ids: A list of feature ids to skip when inferring feature vector schema.
                These ids are linked to features which are part of a Feature Group with embeddings and
                therefore stored in the embedding online store (see vector_db_client).
                The name is kept for consistency with vector_server but should be updated to reflect that
                it is the feature id that is being skipped, not Feature Group (fg).
        """
        self._online_store_rest_client_api = (
            online_store_rest_client_api.OnlineStoreRestClientApi()
        )
        self._features = features
        self._skip_fg_ids = skip_fg_ids
        self._ordered_feature_names_and_dtypes = [
            (feat.name, feat.type)
            for feat in features
            if not (
                feat.label
                or feat.inference_helper_column
                or feat.training_helper_column
                or feat.index in skip_fg_ids
            )
        ]

    def _build_base_payload(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        metadata_options: Optional[Dict[str, bool]] = None,
    ) -> Dict[str, Union[str, Dict[str, bool]]]:
        """Build the base payload for the RonDB REST Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        !!! warning
            featureName and featureType must be set to True to allow the response to be converted
            to a feature vector with convert_rdrs_response_to_dict_feature_vector.

        Args:
            feature_store_name: The name of the feature store in which the feature view is registered.
                The suffix '_featurestore' should be omitted.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_vector" or "response_json".
                If "feature_value_dict" is selected the payload will enforce fetching feature metadata.

        Returns:
            The payload to send to the RonDB REST Server Feature Store API.
        """
        base_payload = {
            "featureStoreName": util.strip_feature_store_suffix(feature_store_name),
            "featureViewName": feature_view_name,
            "featureViewVersion": feature_view_version,
            "options": {
                "validatePassedFeatures": False,
            },
        }

        if metadata_options is None:
            return base_payload
        else:
            base_payload["metadataOptions"] = {
                "featureName": metadata_options.get("featureName", False),
                "featureType": metadata_options.get("featureType", False),
            }
            return base_payload

    def handle_passed_features_dict(
        self, passed_features: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Handle the passed features dictionary to convert event time to timestamp.

        # Arguments:
            passed_features: A dictionary with the feature names as keys and the values to substitute for this specific vector.

        # Returns:
            A dictionary with the feature names as keys and the values to substitute for this specific vector.
        """
        # TODO: Handle prefix for passed features?
        if passed_features is None:
            return {}
        return {
            key: (
                value
                if not isinstance(value, datetime)
                else util.convert_event_time_to_timestamp(value)
            )
            for (key, value) in passed_features.items()
        }

    def get_single_raw_feature_vector(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        entry: Dict[str, Any],
        passed_features: Optional[Dict[str, Any]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> Dict[str, Any]:
        """Get a single feature vector from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        # Arguments:
            feature_store_name: The name of the feature store in which the feature view is registered.
                The suffix '_featurestore' should be omitted.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            entry: A dictionary with the feature names as keys and the primary key as values.
            passed_features: A dictionary with the feature names as keys and the values to substitute for this specific vector.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_value_dict" or "response_json".

        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": The status pertinent to this single feature vector.
                - "features": A list of the feature values.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.

        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            ValueError: If the length of the feature values and metadata in the reponse does not match.
        """
        payload = self._build_base_payload(
            feature_store_name=feature_store_name,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            metadata_options=metadata_options,
        )
        payload["entries"] = entry
        payload["passedFeatures"] = self.handle_passed_features_dict(
            passed_features=passed_features
        )

        response = self._online_store_rest_client_api.get_single_raw_feature_vector(
            payload=payload
        )
        _logger.debug(
            f"get_single_vector rest client raw response: {json.dumps(response, indent=2)}"
        )

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT:
            return self.convert_rdrs_response_to_feature_value_dict(
                row_feature_values=response["features"],
            )
        else:
            return response

    def get_batch_raw_feature_vectors(
        self,
        feature_store_name: str,
        feature_view_name: str,
        feature_view_version: int,
        entries: List[Dict[str, Any]],
        passed_features: Optional[List[Dict[str, Any]]] = None,
        metadata_options: Optional[Dict[str, bool]] = None,
        return_type: str = RETURN_TYPE_FEATURE_VALUE_DICT,
    ) -> List[Dict[str, Any]]:
        """Get a list of feature vectors from the online feature store via RonDB Rest Server Feature Store API.

        Check the RonDB Rest Server Feature Store API documentation for more details:
        https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server

        # Arguments:
            feature_store_name: The name of the feature store in which the feature view is registered.
                The suffix '_featurestore' should be omitted.
            feature_view_name: The name of the feature view from which to retrieve the feature vector.
            feature_view_version: The version of the feature view from which to retrieve the feature vector.
            entries: A list of dictionaries with the feature names as keys and the primary key as values.
            passed_features: A list of dictionaries with the feature names as keys and the values to substitute.
                Note that the list should be ordered in the same way as the entries list.
            metadata_options: Whether to include feature metadata in the response.
                Keys are "featureName" and "featureType" and values are boolean.
            return_type: The type of the return value. Either "feature_value_dict" or "response_json".

        # Returns:
            The response json containing the feature vector as well as status information
            and optionally descriptive metadata about the features. It contains the following fields:
                - "status": A list of the status for each feature vector retrieval.
                - "features": A list containing list of the feature values for each feature_vector.
                - "metadata": A list of dictionaries with metadata for each feature. The order should match the order of the features.

        # Raises:
            `hsfs.client.exceptions.RestAPIError`: If the server response status code is not 200.
            `ValueError`: If the length of the passed features does not match the length of the entries.
        """
        payload = self._build_base_payload(
            feature_store_name=feature_store_name,
            feature_view_name=feature_view_name,
            feature_view_version=feature_view_version,
            metadata_options=metadata_options,
        )
        payload["entries"] = entries
        if isinstance(passed_features, list) and (
            len(passed_features) == len(entries) or len(passed_features) == 0
        ):
            payload["passedFeatures"] = [
                self.handle_passed_features_dict(passed_features=passed_feature)
                for passed_feature in passed_features
            ]
        elif passed_features is None:
            payload["passedFeatures"] = []
        else:
            raise ValueError(
                "Length of passed features does not match the length of the entries."
            )

        response = self._online_store_rest_client_api.get_batch_raw_feature_vectors(
            payload=payload
        )
        _logger.debug(response)

        if return_type == self.RETURN_TYPE_FEATURE_VALUE_DICT:
            return self.convert_batch_response_to_feature_value_dict(
                batch_response=response
            )
        else:
            return response

    def convert_batch_response_to_feature_value_dict(
        self, batch_response: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Split the response from the RonDB Rest Server Feature Store API to convert each feature vector to a dictionary.

        Skip the feature vectors that have an error status.

        # Arguments:
            batch_response: The response from the RonDB Rest Server Feature Store API.

        # Returns:
            A list of dictionaries with the feature names as keys and the feature values as values.
        """
        return [
            self.convert_rdrs_response_to_feature_value_dict(
                row_feature_values=row,
            )
            for row in batch_response["features"]
        ]

    def convert_rdrs_response_to_feature_value_dict(
        self,
        row_feature_values: Union[List[Any], None],
    ) -> Dict[str, Any]:
        """Convert the response from the RonDB Rest Server Feature Store API to a feature:value dict.

        When RonDB Server encounter an error it may send a null value for the feature vector. This function
        will handle this case and return a dictionary with None values for all feature names.

        # Arguments:
            row_feature_values: A list of the feature values.

        # Returns:
            A dictionary with the feature names as keys and the feature values as values. Values types are not guaranteed to
            match the feature type in the metadata. Timestamp SQL types are converted to python datetime.
        """
        # An argument could be made that passed features are actually set in this vector.
        if row_feature_values is None:
            return {name: None for name, _ in self._ordered_feature_names_and_dtypes}
        return {
            name: (
                vector_value
                if (dtype != "timestamp" or vector_value is None)
                else self.handle_timestamp_based_on_dtype(vector_value)
            )
            for vector_value, (name, dtype) in zip(
                row_feature_values, self._ordered_feature_names_and_dtypes
            )
        }

    def handle_timestamp_based_on_dtype(
        self, timestamp_value: Union[str, int]
    ) -> Optional[datetime]:
        """Handle the timestamp based on the dtype which is returned.

        Currently timestamp which are in the database are returned as string. Whereas
        passed features which were given as datetime are returned as integer timestamp.

        # Arguments:
            timestamp_value: The timestamp value to be handled, either as int or str.
        """
        if isinstance(timestamp_value, int):
            return datetime.fromtimestamp(
                timestamp_value / 1000, tz=timezone.utc
            ).replace(tzinfo=None)
        elif isinstance(timestamp_value, str):
            return datetime.strptime(timestamp_value, self.SQL_TIMESTAMP_STRING_FORMAT)
        else:
            raise ValueError(
                f"Timestamp value {timestamp_value} was expected to be of type int or str."
            )
