from __future__ import print_function

from dbt.adapters.factory import get_adapter
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.runner import RunManager
from dbt.node_types import NodeType
from dbt.node_runners import ModelRunner

import dbt.ui.printer
from dbt.task.compile import CompileTask

class CompareTask(CompileTask):
    def _get_manifest(self):
        compiler = dbt.compilation.Compiler(self.config)
        compiler.initialize()

        all_projects = compiler.get_all_projects()

        manifest = dbt.loader.GraphLoader.load_all(self.config, all_projects)
        return manifest

    def run(self):

        logger.info('dbt compare: Comparing local codebase to catalog in data warehouse.')

        manifest = self._get_manifest()

        # Look up all of the relations dbt knows about
        used_schemas = manifest.get_used_schemas()
        used_relations = []
        for node in manifest.nodes.items():
            node = node[1].to_dict()
            if node['resource_type'] == 'model' and node['config']['enabled']:
                used_relations.append("%s.%s" % (node['schema'], node['alias']))

        # Look up all of the relations in the DB
        adapter = get_adapter(self.config)
        results = adapter.get_unfiltered_catalog(manifest)

        results = [
            dict(zip(results.column_names, row))
            for row in results
        ]

        # Filter down to only schemas we know about
        results = [x for x in results if x['table_schema'] in used_schemas]

        existing_relations = {}
        for x in results:
            name = "%s.%s" % (x['table_schema'], x['table_name'])
            existing_relations[name] = x['table_type']

        problems = {}
        for k,v in existing_relations.items():
            if k not in used_relations:
                problems[k] = v

        if len(problems) == 0:
            logger.info("All clear! The catalog matches the manifest.")
        else:
            logger.info("-"*40)
            logger.info("Warning: We found some discrepancies between the database catalog and the dbt manifest:")
            logger.info("This may indicate that you have outdated relations in your warehouse that can be removed.")
            logger.info("-"*40)


        for k,v in problems.items():
            logger.info("%s: %s" % (k, v))
            logger.info("-"*40)

        return problems
