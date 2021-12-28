## Consumer Failure behaviour

### Continuation behavior

`:continuation`

- continue without stopping (`:non-stop`)
- stop when exception is thrown (default) (`:stop-on-failure`)

### What to do failed event itself?

`:failed-event-stream`

- put it on another stream - same key/value as original event
- or ignore (default)

### What to do with the exception?

`:failed-exception-stream`

- put it on another stream (with event as well) - same key as original
  event, value as `{:event <value>, :exception <ex>}`
- or ignore (default)

### Kafka manual commit behavior

`:kkiss.engine.kafka/commit-behavior`

- assume auto-commit (`:do-not-manually-commit`)
- commit to latest before failure (default)
  (`:latest-before-failure` - `:continuation` must be
  `:stop-on-failure` - rationale is if it's continuing then can't be
  manual commit)
