network-transport-cci
=====================

This project adds support for many new high performance network
interconnects, such as Infiniband, to Cloud Haskell. This is done via
binding to CCI, a simple and portable API unifying the programming
models for many network interconnects.

Setup
-----

This package can be installed with the following command:

    $ cabal install

Note that the `cci` package is a dependency of this package. It must
be installed first, either from the [source repository][haskell-cci]
or from Hackage.

[haskell-cci]: http://github.com/ps-labs/haskell-cci
