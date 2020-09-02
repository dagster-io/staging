# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots['test_query_all_solids 1'] = {
    'repositoryOrError': {
        'id': '4497bd467cf4af860afa9c962396f415ca1c1b42',
        'location': {
            'environmentPath': None,
            'isReloadSupported': False,
            'name': '<<in_process>>'
        },
        'origin': {
            '__typename': 'PythonRepositoryOrigin',
            'codePointer': {
                'description': '/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql_tests/graphql/setup.py::test_repo',
                'metadata': [
                    {
                        'key': 'python_file',
                        'value': '/Users/prha/code/dagster/python_modules/dagster-graphql/dagster_graphql_tests/graphql/setup.py'
                    },
                    {
                        'key': 'attribute',
                        'value': 'test_repo'
                    }
                ]
            },
            'executablePath': '/Users/prha/.pyenv/versions/3.6.8/envs/dagster-3.6.8/bin/python3.6'
        }
    }
}
