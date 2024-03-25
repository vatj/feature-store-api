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
import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Union

from hsfs import feature_view, training_dataset, util
from hsfs.constructor import serving_prepared_statement
from hsfs.core import feature_view_api, storage_connector_api, training_dataset_api
from hsfs.serving_key import ServingKey
from sqlalchemy import bindparam, exc, sql, text


_logger = logging.getLogger(__name__)


class OnlineStoreSqlClient:
    def __init__(self, feature_store_id: id):
        _logger.info("Initializing OnlineStoreSqlClient")
        self._feature_store_id = feature_store_id
        self._prefix_by_serving_index = None
        self._prepared_statement_engine = None
        self._prepared_statements = None
        self._helper_column_prepared_statements = None
        self._pkname_by_serving_index = None
        self._valid_serving_key = None
        self._serving_key_by_serving_index = {}
        self._async_pool = None
        self._external = True

        self._feature_view_api = feature_view_api.FeatureViewApi(feature_store_id)
        self._training_dataset_api = training_dataset_api.TrainingDatasetApi(
            feature_store_id
        )
        self._storage_connector_api = storage_connector_api.StorageConnectorApi()

    def fetch_prepared_statements(
        self,
        entity: Union["feature_view.FeatureView", "training_dataset.TrainingDataset"],
        inference_helper_columns: bool,
    ):
        if isinstance(entity, feature_view.FeatureView):
            _logger.debug(
                f"Initializing prepared statements for feature view {entity.name} version {entity.version}."
            )
            if inference_helper_columns:
                _logger.debug(
                    "Inference helper columns are enabled, fetching corresponding prepared statements."
                )
                self._helper_column_prepared_statements = (
                    self._feature_view_api.get_serving_prepared_statement(
                        entity.name,
                        entity.version,
                        batch=False,
                        inference_helper_columns=True,
                    )
                )
                self._batch_helper_column_prepared_statements = (
                    self._feature_view_api.get_serving_prepared_statement(
                        entity.name,
                        entity.version,
                        batch=True,
                        inference_helper_columns=True,
                    )
                )
            _logger.debug("Fetching prepared statements for feature vectors.")
            self._prepared_statements = (
                self._feature_view_api.get_serving_prepared_statement(
                    entity.name,
                    entity.version,
                    batch=False,
                    inference_helper_columns=False,
                )
            )
            self._batch_prepared_statements = (
                self._feature_view_api.get_serving_prepared_statement(
                    entity.name,
                    entity.version,
                    batch=True,
                    inference_helper_columns=False,
                )
            )
        elif isinstance(entity, training_dataset.TrainingDataset):
            _logger.debug(
                f"Fetching prepared statements for training dataset {entity.name} version {entity.version}."
            )
            self._prepared_statements = (
                self._training_dataset_api.get_serving_prepared_statement(
                    entity, batch=False
                )
            )
            self._batch_prepared_statements = (
                self._training_dataset_api.get_serving_prepared_statement(
                    entity, batch=True
                )
            )
        else:
            raise ValueError(
                "Object type needs to be `feature_view.FeatureView` or `training_dataset.TrainingDataset`."
            )

    def init_prepared_statement(
        self,
        entity: Union["feature_view.FeatureView", "training_dataset.TrainingDataset"],
        external: bool,
        inference_helper_columns: bool,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        _logger.debug(
            "Fetch and reset prepared statements and external as user may be re-initialising with different parameters"
        )
        self.fetch_prepared_statements(entity, inference_helper_columns)
        self._external = external
        _logger.debug("Acquiring or starting event loop for async engine.")
        loop = asyncio.get_event_loop()
        _logger.debug(f"Setting up aiomysql connection, with options : {options}")
        loop.run_until_complete(
            self._set_aiomysql_connection(
                len(self._prepared_statements), options=options
            )
        )

        self._prepared_statements = self._parametrize_prepared_statements(
            self._prepared_statements, batch=False
        )
        # Use the prepared statements to initialize the serving utils
        self.init_parametrize_and_serving_utils()
        self._batch_prepared_statements, _ = self._parametrize_prepared_statements(
            self._batch_prepared_statements, batch=True
        )

        if inference_helper_columns:
            self._helper_column_prepared_statements = (
                self._parametrize_prepared_statements(
                    self._helper_column_prepared_statements, batch=False
                )
            )
            self._batch_helper_column_prepared_statements = (
                self._parametrize_prepared_statements(
                    self._batch_helper_column_prepared_statements, batch=True
                )
            )

    def init_parametrize_and_serving_utils(self):
        self._prefix_by_serving_index = {}
        self._feature_name_order_by_psp = {}
        for statement in self._prepared_statements:
            if statement.feature_group_id in self._skip_fg_ids:
                continue

        self._prefix_by_serving_index[statement.prepared_statement_index] = (
            statement.prefix
        )
        self._feature_name_order_by_psp[statement.prepared_statement_index] = dict(
            [(psp.name, psp.index) for psp in statement.prepared_statement_parameters]
        )
        pk_names = [
            psp.name
            for psp in sorted(
                statement.prepared_statement_parameters,
                key=lambda psp: psp.index,
            )
        ]
        # construct serving key if it is not provided.
        if self._serving_keys is None:
            serving_keys = set()
            for pk_name in pk_names:
                # should use `ignore_prefix=True` because before hsfs 3.3,
                # only primary key of a feature group are matched in
                # user provided entry.
                serving_key = ServingKey(
                    feature_name=pk_name,
                    join_index=statement.prepared_statement_index,
                    prefix=statement.prefix,
                    ignore_prefix=True,
                )
                serving_keys.add(serving_key)

        for sk in self._serving_keys:
            self._serving_key_by_serving_index[sk.join_index] = (
                self._serving_key_by_serving_index.get(sk.join_index, []) + [sk]
            )

        # sort the serving by PreparedStatementParameter.index
        for join_index in self._serving_key_by_serving_index:
            # feature_name_order_by_psp do not include the join index when the joint feature only contains label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if join_index in self._feature_name_order_by_psp:
                self._serving_key_by_serving_index[join_index] = sorted(
                    self._serving_key_by_serving_index[join_index],
                    key=lambda _sk,
                    join_index=join_index: self._feature_name_order_by_psp[
                        join_index
                    ].get(_sk.feature_name, 0),
                )

        self._valid_serving_key = set(
            [sk.feature_name for sk in self._serving_keys]
            + [sk.required_serving_key for sk in self._serving_keys]
        )

    def _parametrize_prepared_statements(self, prepared_statements, batch):
        prepared_statements_dict = {}
        for prepared_statement in prepared_statements:
            if prepared_statement.feature_group_id in self._skip_fg_ids:
                continue
            query_online = str(prepared_statement.query_online).replace("\n", " ")

            pk_names = [
                psp.name
                for psp in sorted(
                    prepared_statement.prepared_statement_parameters,
                    key=lambda psp: psp.index,
                )
            ]

            if not batch:
                for pk_name in pk_names:
                    query_online = self._parametrize_query(pk_name, query_online)
                query_online = sql.text(query_online)
            else:
                query_online = self._parametrize_query("batch_ids", query_online)
                query_online = sql.text(query_online)
                query_online = query_online.bindparams(
                    batch_ids=bindparam("batch_ids", expanding=True)
                )

            prepared_statements_dict[prepared_statement.prepared_statement_index] = (
                query_online
            )

        return prepared_statements_dict

    def get_single_feature_vector(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve single vector with parallel queries using aiomysql engine."""
        return self._single_vector_result(entry, self._prepared_statements)

    def get_batch_feature_vector(self, entries: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve batch vector with parallel queries using aiomysql engine."""
        return self._batch_vector_results(entries, self._batch_prepared_statements)

    def get_inference_helper_vector(self, entry: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve single vector with parallel queries using aiomysql engine."""
        return self._single_vector_result(
            entry, self._helper_column_prepared_statements
        )

    def get_batch_inference_helper_vector(
        self, entries: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Retrieve batch vector with parallel queries using aiomysql engine."""
        return self._batch_vector_results(
            entries, self._batch_helper_column_prepared_statements
        )

    def _single_vector_result(
        self, entry, prepared_statement_objects
    ) -> Dict[str, Any]:
        """Retrieve single vector with parallel queries using aiomysql engine."""

        if all([isinstance(val, list) for val in entry.values()]):
            raise ValueError(
                "Entry is expected to be single value per primary key. "
                "If you have already initialised prepared statements for single vector and now want to retrieve "
                "batch vector please reinitialise prepared statements with  "
                "`training_dataset.init_prepared_statement()` "
                "or `feature_view.init_serving()`"
            )
        self._validate_serving_key(entry)
        # Initialize the set of values
        serving_vector = {}
        bind_entries = {}
        prepared_statement_execution = {}
        for prepared_statement_index in prepared_statement_objects:
            pk_entry = {}
            next_statement = False
            for sk in self._serving_key_by_serving_index[prepared_statement_index]:
                if sk.required_serving_key not in entry.keys():
                    # Check if there is any entry matched with feature name.
                    if sk.feature_name in entry.keys():
                        pk_entry[sk.feature_name] = entry[sk.feature_name]
                    else:
                        # User did not provide the necessary serving keys, we expect they have
                        # provided the necessary features as passed_features.
                        # We are going to check later if this is true
                        next_statement = True
                        break
                else:
                    pk_entry[sk.feature_name] = entry[sk.required_serving_key]
            if next_statement:
                continue
            bind_entries[prepared_statement_index] = pk_entry
            prepared_statement_execution[prepared_statement_index] = (
                prepared_statement_objects[prepared_statement_index]
            )

        # run all the prepared statements in parallel using aiomysql engine
        _logger.debug(
            f"Executing prepared statements for serving vector with entries: {bind_entries}"
        )
        loop = asyncio.get_event_loop()
        results_dict = loop.run_until_complete(
            self._execute_prep_statements(prepared_statement_execution, bind_entries)
        )
        _logger.debug(f"Retrieved feature vectors: {results_dict}")

        _logger.debug("Constructing serving vector from results")
        for key in results_dict:
            for row in results_dict[key]:
                _logger.debug(f"Processing row: {row} for prepared statement {key}")
                result_dict = dict(row)
                serving_vector.update(result_dict)

        return serving_vector

    def _batch_vector_results(self, entries, prepared_statement_objects):
        """Execute prepared statements in parallel using aiomysql engine."""
        _logger.info(
            f"Starting batch vector retrieval for {len(entries)} entries via aiomysql engine."
        )
        # create dict object that will have of order of the vector as key and values as
        # vector itself to stitch them correctly if there are multiple feature groups involved. At this point we
        # expect that backend will return correctly ordered vectors.
        batch_results = [{} for _ in range(len(entries))]
        entry_values = {}
        serving_keys_all_fg = []
        prepared_stmts_to_execute = {}
        # construct the list of entry values for binding to query
        _logger.debug(f"Parametrize prepared statements with entry values: {entries}")
        for prepared_statement_index in prepared_statement_objects:
            # prepared_statement_index include fg with label only
            # But _serving_key_by_serving_index include the index when the join_index is 0 (left side)
            if prepared_statement_index not in self._serving_key_by_serving_index:
                continue

            prepared_stmts_to_execute[prepared_statement_index] = (
                prepared_statement_objects[prepared_statement_index]
            )
            entry_values_tuples = list(
                map(
                    lambda e, prepared_statement_index=prepared_statement_index: tuple(
                        [
                            (
                                e.get(sk.required_serving_key)
                                # Check if there is any entry matched with feature name,
                                # if the required serving key is not provided.
                                or e.get(sk.feature_name)
                            )
                            for sk in self._serving_key_by_serving_index[
                                prepared_statement_index
                            ]
                        ]
                    ),
                    entries,
                )
            )
            _logger.debug(
                f"Prepared statement {prepared_statement_index} with entries: {entry_values_tuples}"
            )
            entry_values[prepared_statement_index] = {"batch_ids": entry_values_tuples}

        _logger.debug(
            f"Executing prepared statements for batch vector with entries: {entry_values}"
        )
        # run all the prepared statements in parallel using aiomysql engine
        loop = asyncio.get_event_loop()
        parallel_results = loop.run_until_complete(
            self._execute_prep_statements(prepared_stmts_to_execute, entry_values)
        )

        _logger.debug(f"Retrieved feature vectors: {parallel_results}, stitching them.")
        # construct the results
        for prepared_statement_index in prepared_stmts_to_execute:
            statement_results = {}
            serving_keys = self._serving_key_by_serving_index[prepared_statement_index]
            serving_keys_all_fg += serving_keys
            prefix_features = [
                (self._prefix_by_serving_index[prepared_statement_index] or "")
                + sk.feature_name
                for sk in self._serving_key_by_serving_index[prepared_statement_index]
            ]
            _logger.debug(
                f"Use prefix from prepare statement because prefix from serving key is collision adjusted {prefix_features}."
            )
            _logger.debug("iterate over results by index of the prepared statement")
            for row in parallel_results[prepared_statement_index]:
                _logger.debug(f"Processing row: {row}")
                row_dict = dict(row)
                # can primary key be complex feature? No, not supported.
                result_dict = row_dict
                _logger.debug(
                    f"Add result to statement results: {self._get_result_key(prefix_features, row_dict)} : {result_dict}"
                )
                statement_results[self._get_result_key(prefix_features, row_dict)] = (
                    result_dict
                )

            _logger.debug(f"Add partial results to batch results: {statement_results}")
            for i, entry in enumerate(entries):
                _logger.debug(
                    "Processing entry %s : %s",
                    entry,
                    statement_results.get(
                        self._get_result_key_serving_key(serving_keys, entry), {}
                    ),
                )
                batch_results[i].update(
                    statement_results.get(
                        self._get_result_key_serving_key(serving_keys, entry), {}
                    )
                )
        return batch_results, serving_keys_all_fg

    def refresh_mysql_connection(self):
        try:
            with self._prepared_statement_engine.connect():
                # This will raise an exception if the connection is closed
                pass
        except exc.OperationalError:
            self._set_mysql_connection()

    def _make_preview_statement(self, statement, n):
        return text(statement.text[: statement.text.find(" WHERE ")] + f" LIMIT {n}")

    def _set_mysql_connection(self, options=None):
        online_conn = self._storage_connector_api.get_online_connector(
            self._feature_store_id
        )
        self._prepared_statement_engine = util.create_mysql_engine(
            online_conn, self._external, options=options
        )

    def _validate_serving_key(self, entry):
        _logger.debug(f"Validating serving key {entry}")
        for key in entry:
            if key not in self._valid_serving_key:
                raise ValueError(
                    f"'{key}' is not a correct serving key. Expect one of the"
                    f" followings: [{', '.join(self._valid_serving_key)}]"
                )

    def filter_entry_by_join_index(self, entry, join_index):
        _logger.debug(f"Filtering entry {entry} by join index {join_index}")
        fg_entry = {}
        complete = True
        for sk in self._serving_key_by_serving_index[join_index]:
            fg_entry[sk.feature_name] = entry.get(sk.required_serving_key) or entry.get(
                sk.feature_name
            )  # fallback to use raw feature name
            if fg_entry[sk.feature_name] is None:
                complete = False
                break
        return complete, fg_entry

    @staticmethod
    def _parametrize_query(name: str, query_online: str) -> str:
        # Now we have ordered pk_names, iterate over it and replace `?` with `:feature_name` one by one.
        # Regex `"^(.*?)\?"` will identify 1st occurrence of `?` in the sql string and replace it with provided
        # feature name, e.g. `:feature_name`:. As we iteratively update `query_online` we are always aiming to
        # replace 1st occurrence of `?`. This approach can only work if primary key names are sorted properly.
        # Regex `"^(.*?)\?"`:
        # `^` - asserts position at start of a line
        # `.*?` - matches any character (except for line terminators). `*?` Quantifier —
        # Matches between zero and unlimited times, expanding until needed, i.e 1st occurrence of `\?`
        # character.
        _logger.debug(f"Parametrizing name {name} in query {query_online}")
        return re.sub(
            r"^(.*?)\?",
            r"\1:" + name,
            query_online,
        )

    @staticmethod
    def _get_result_key(primary_keys, result_dict):
        result_key = []
        for pk in primary_keys:
            result_key.append(result_dict.get(pk))
        return tuple(result_key)

    @staticmethod
    def _get_result_key_serving_key(serving_keys, result_dict):
        result_key = []
        for sk in serving_keys:
            result_key.append(
                result_dict.get(sk.required_serving_key)
                or result_dict.get(sk.feature_name)
            )
        return tuple(result_key)

    async def _set_aiomysql_connection(self, default_min_size: int, options=None):
        _logger.debug(
            "Fetching storage connector for sql connection to Online Feature Store."
        )
        online_connector = self._storage_connector_api.get_online_connector(
            self._feature_store_id
        )
        _logger.debug(
            f"Creating async engine with options: {options} and default min size: {default_min_size}"
        )
        self._async_pool = await util.create_async_engine(
            online_connector, self._external, default_min_size, options=options
        )

    async def _query_async_sql(self, stmt, bind_params):
        """Query prepared statement together with bind params using aiomysql connection pool"""
        # Get a connection from the pool
        async with self._async_pool.acquire() as conn:
            # Execute the prepared statement
            cursor = await conn.execute(stmt, bind_params)
            # Fetch the result
            resultset = await cursor.fetchall()
            await cursor.close()

        return resultset

    async def _execute_prep_statements(self, prepared_statements: dict, entries: dict):
        """Iterate over prepared statements to create async tasks
        and gather all tasks results for a given list of entries."""

        # validate if prepared_statements and entries have the same keys
        if prepared_statements.keys() != entries.keys():
            # iterate over prepared_statements and entries to find the missing key
            # remove missing keys from prepared_statements
            for key in list(prepared_statements.keys()):
                if key not in entries:
                    prepared_statements.pop(key)

        tasks = [
            asyncio.ensure_future(
                self._query_async_sql(prepared_statements[key], entries[key])
            )
            for key in prepared_statements
        ]
        # Run the queries in parallel using asyncio.gather
        results = await asyncio.gather(*tasks)
        results_dict = {}
        # Create a dict of results with the prepared statement index as key
        for i, key in enumerate(prepared_statements):
            results_dict[key] = results[i]
        # TODO: close connection? closing connection pool here will make un-acquirable for future connections
        # unless init_serving is explicitly called before get_feature_vector(s).
        return results_dict

    @property
    def feature_store_id(self) -> int:
        return self._feature_store_id

    @property
    def prepared_statement_engine(self):
        """JDBC connection engine to retrieve connections to online features store from."""
        return self._prepared_statement_engine

    @prepared_statement_engine.setter
    def prepared_statement_engine(self, prepared_statement_engine):
        self._prepared_statement_engine = prepared_statement_engine

    @property
    def prepared_statements(
        self,
    ) -> Dict[int, "serving_prepared_statement.ServingPreparedStatement"]:
        """The dict object of prepared_statements as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset. Used for single vector retrieval.
        """
        return self._prepared_statements

    @prepared_statements.setter
    def prepared_statements(
        self,
        prepared_statements: Dict[
            int, "serving_prepared_statement.ServingPreparedStatement"
        ],
    ) -> None:
        self._prepared_statements = prepared_statements

    @property
    def batch_prepared_statements(
        self,
    ) -> Dict[int, "serving_prepared_statement.ServingPreparedStatement"]:
        """The dict object of prepared_statements as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset. Used for batch retrieval.
        """
        return self._batch_prepared_statements

    @batch_prepared_statements.setter
    def batch_prepared_statements(
        self,
        batch_prepared_statements: Dict[
            int, "serving_prepared_statement.ServingPreparedStatement"
        ],
    ) -> None:
        self._batch_prepared_statements = batch_prepared_statements

    @property
    def helper_column_prepared_statements(
        self,
    ) -> Optional[Dict[int, "serving_prepared_statement.ServingPreparedStatement"]]:
        """The dict object of prepared_statements as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset. Used for single vector retrieval.
        """
        return self._helper_column_prepared_statements

    @helper_column_prepared_statements.setter
    def helper_column_prepared_statements(
        self,
        helper_column_prepared_statements: Dict[
            int, "serving_prepared_statement.ServingPreparedStatement"
        ],
    ) -> None:
        self._helper_column_prepared_statements = helper_column_prepared_statements

    @property
    def batch_helper_column_prepared_statements(
        self,
    ) -> Optional[Dict[int, "serving_prepared_statement.ServingPreparedStatement"]]:
        """The dict object of prepared_statements as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset. Used for single vector retrieval.
        """
        return self._helper_column_prepared_statements

    @batch_helper_column_prepared_statements.setter
    def batch_helper_column_prepared_statements(
        self,
        batch_helper_column_prepared_statements: Dict[
            int, "serving_prepared_statement.ServingPreparedStatement"
        ],
    ) -> None:
        self._batch_helper_column_prepared_statements = (
            batch_helper_column_prepared_statements
        )

    @property
    def prefix_by_serving_index(self) -> Dict[int, str]:
        """The dict object of prefixes as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._prefix_by_serving_index

    @prefix_by_serving_index.setter
    def prefix_by_serving_index(self, prefix_by_serving_index: Dict[int, str]) -> None:
        self._prefix_by_serving_index = prefix_by_serving_index

    @property
    def serving_key_by_serving_index(
        self,
    ) -> Dict[int, List["ServingKey"]]:
        """The dict object of serving keys as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._serving_key_by_serving_index

    @property
    def feature_name_order_by_psp(self) -> Dict[int, Dict[str, int]]:
        """The dict object of feature names as values and keys as indices of positions in the query for
        selecting features from feature groups of the training dataset.
        """
        return self._feature_name_order_by_psp

    @property
    def training_dataset_api(self) -> training_dataset_api.TrainingDatasetApi:
        return self._training_dataset_api

    @property
    def feature_view_api(self) -> feature_view_api.FeatureViewApi:
        return self._feature_view_api