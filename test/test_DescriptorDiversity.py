"""MMR must penalize duplicate-like candidates while retaining relevance."""
from unittest.mock import Mock

import numpy as np
import pytest

from aperturedb.Descriptors import Descriptors


@pytest.mark.parametrize('metric', ['CS', 'IP', 'L2'])
@pytest.mark.parametrize('weight,expected', [(0.0, ['query', 'different']),
                                             (1.0, ['query', 'duplicate'])])
def test_mmr_balances_relevance_and_diversity(metric, weight, expected):
    descriptors = Descriptors(client=Mock())

    def candidates(*args, **kwargs):
        descriptors.response = [
            {'name': 'query', 'vector': np.array([1., 0.], dtype=np.float32)},
            {'name': 'duplicate', 'vector': np.array(
                [.99, .1], dtype=np.float32)},
            {'name': 'different', 'vector': np.array(
                [0., 1.], dtype=np.float32)},
        ]
    descriptors.find_similar = Mock(side_effect=candidates)
    descriptors.find_similar_mmr('Vectors', [1., 0.], 2, 3,
                                 lambda_mult=weight, metric=metric)
    assert [row['name'] for row in descriptors.response] == expected
