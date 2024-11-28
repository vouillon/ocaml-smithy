# ocaml-smithy

This aims to provide eventually a comprehensive AWS SDK for OCaml. However, signing and and performing operations is not implemented yet, and some protocols still need work.

The SDK is code-generated from [Smithy models](https://awslabs.github.io/smithy/) that represent each AWS service. The models were copied from [smithy-rs](https://github.com/smithy-lang/smithy-rs).

First, this will generate the code:
```
dune exec ./smithy.exe
```

To run the tests:
```
dune runtest
```

To build the documentation:
```
dune build @doc
```
