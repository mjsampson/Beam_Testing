import os


class DefaultConfig(object):
    """Setup global configuration vars."""

    BITCOIN_HOST = os.environ.get('BITCOIN_HOST')

    BITCOIN_RPCUSER = os.environ.get('BITCOIN_RPCUSER')

    BITCOIN_RPCPASSWORD = os.environ.get('BITCOIN_RPCPASSWORD')
