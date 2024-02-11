#
#   Copyright 2024 Logical Clocks AB
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
from hsfs.core import rondb_api


class RonDBEngine:
    # TODO: Fetch the API version from Hopsworks Cluster Configuration
    _API_VERSION = "0.0.1"

    def __init__(self, feature_store_name: str):
        self._rondb_api = rondb_api.RonDBApi()
        self.feature_store_name = feature_store_name

    def payload_default_builder(
        self,
        fv_name: str,
        fv_version: int,
        feature_type: bool = False,
        feature_name: bool = False,
    ):
        return {
            "featureViewName": fv_name,
            "featureViewVersion": fv_version,
            "featureStoreName": self.feature_store_name,
            "passedFeatures": {},
            "metadataOptions": {
                "featureType": feature_type,
                "featureName": feature_name,
            },
        }

    def get_raw_feature_vector(self, fv_name: str, fv_version: int, primary_key: str):
        payload = self.payload_default_builder(fv_name, fv_version)
        payload["entries"] = {"id1": primary_key}
        return self._rondb_api.get_feature_vector(self._API_VERSION, payload)

    def get_raw_feature_vectors(
        self, fv_name: str, fv_version: int, primary_keys: list[str]
    ):
        payload = self.payload_default_builder(fv_name, fv_version)
        payload["entries"] = [{"id1": pk} for pk in primary_keys]
        return self._rondb_api.get_feature_vectors(self._API_VERSION, payload)
