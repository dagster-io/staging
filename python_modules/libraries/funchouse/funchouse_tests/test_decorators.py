from funchouse import asset


def test_asset():
    """TODO"""

    @asset
    def _my_asset():
        return 1


def test_asset_with_context_arg():
    """TODO"""
