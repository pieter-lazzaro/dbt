from __future__ import print_function

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_types import NodeType
from dbt.node_runners import ModelRunner
from dbt.utils import is_enabled as check_is_enabled

import dbt.ui.printer
from dbt.task.base_task import BaseTask


class CompareTask(BaseTask):
    def _get_manifest(self):
        compiler = dbt.compilation.Compiler(self.config)
        compiler.initialize()

        all_projects = compiler.get_all_projects()

        manifest = dbt.loader.GraphLoader.load_all(self.config, all_projects)
        return manifest

    def run(self):

        logger.info(
            "dbt compare: Comparing local codebase to catalog in data warehouse."
        )

        manifest = self._get_manifest()

        # Look up all of the relations dbt knows about
        model_relations = set()
        for node in manifest.nodes.values():
            node = node.to_dict()
            is_refable = node["resource_type"] in NodeType.refable()
            is_enabled = check_is_enabled(node)
            is_ephemeral = node["config"]["materialized"] == "ephemeral"
            if is_refable and is_enabled and not is_ephemeral:
                rel = (node["schema"], node["alias"])
                model_relations.add(rel)

        # Look up all of the relations in the DB
        adapter = get_adapter(self.config)
        results = adapter.get_catalog(manifest)

        results = [dict(zip(results.column_names, row)) for row in results]

        database_relations = set()
        for row in results:
            rel = (row["table_schema"], row["table_name"])
            database_relations.add(rel)

        checked_schemas = set([x[0] for x in database_relations])

        logger.info("-" * 40)
        logger.info("dbt compare reviewed the following schemas:")
        for schema_name in checked_schemas:
            logger.info(schema_name)
        logger.info("-" * 40)

        problems = database_relations - model_relations

        if len(problems) == 0:
            logger.info(
                "All clear! There are no relations in the checked schemas that are not defined in dbt models."
            )
        else:
            logger.info("-" * 40)
            logger.info(
                "Warning: We found some discrepancies between the database catalog and the dbt models:"
            )
            logger.info(
                "This may indicate that you have outdated relations in your warehouse that can be removed."
            )
            logger.info("-" * 40)

        for relation in problems:
            logger.info("DIFFERENCE: %s.%s" % (relation[0], relation[1]))
        logger.info("-" * 40)

        return problems
