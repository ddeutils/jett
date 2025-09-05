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

## Metric

This is the main component of this project. I want to focus on this feature because
it should to show all data that want to know when start execute some engine tool.

- What happened?
- What changed?

```text
engine/
source/
transforms/
    op/
    group/
    op/
    op/
    .../
sink/
```
