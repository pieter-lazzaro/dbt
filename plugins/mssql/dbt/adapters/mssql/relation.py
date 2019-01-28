from dbt.adapters.base.relation import BaseRelation
import dbt.utils


class MssqlRelation(BaseRelation):
    DEFAULTS = {
        'metadata': {
            'type': 'MssqlRelation'
        },
        'start_quote_character': '[',
        'end_quote_character': ']',
        'quote_policy': {
            'database': True,
            'schema': True,
            'identifier': True,
        },
        'include_policy': {
            'database': False,
            'schema': True,
            'identifier': True,
        }
    }

    SCHEMA = {
        'type': 'object',
        'properties': {
            'metadata': {
                'type': 'object',
                'properties': {
                    'type': {
                        'type': 'string',
                        'const': 'MssqlRelation',
                    },
                },
            },
            'type': {
                'enum': BaseRelation.RelationTypes + [None],
            },
            'path': BaseRelation.PATH_SCHEMA,
            'include_policy': BaseRelation.POLICY_SCHEMA,
            'quote_policy': BaseRelation.POLICY_SCHEMA,
            'start_quote_character': {'type': 'string'},
            'end_quote_character': {'type': 'string'},
        },
        'required': ['metadata', 'type', 'path', 'include_policy',
                     'quote_policy', 'start_quote_character',
                     'end_quote_character']
    }

    
    def quoted(self, identifier):
        return '{start_quote_char}{identifier}{end_quote_char}'.format(
            start_quote_char=self.start_quote_character,
            end_quote_char=self.end_quote_character,
            identifier=identifier)
