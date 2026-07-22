# internal/storage/mock

This package contains the hand-written, concurrency-safe fake for
`storage.Store`. Tests configure only the function fields they exercise; unset
methods return harmless zero values. Every call is recorded and can be checked
with `Calls` or `CallCount`.

```go
store := &mock.Store{
    UpsertJobFn: func(_ context.Context, job storage.Job) (storage.Job, error) {
        job.ID = 1
        return job, nil
    },
}
```
