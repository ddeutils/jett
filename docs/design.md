# Design

This project design support multi-engine template tool (not only DataFrame engine).

```text
S --> Select Engine --> DataFrame  --> Prepare Model --> Execute --> E
                    --> DBT
                    --> GX
```

This project will focus on metric and json schema support first. The mean it does
not strict the way to use each engine that the currently I use source, transform, sink
layers.
