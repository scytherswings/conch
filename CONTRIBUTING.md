# Contributing

This is a small project with a few unusual, load-bearing constraints. They are
not style preferences — each one exists because breaking it makes a failover
supervisor less trustworthy. Please read them before opening a PR.

## Run this before you push

```sh
make test    # go test ./... and the arbiter suite
make lint    # gofmt and go vet
make validate
```

Or run the raw commands:

```sh
go build ./... && go vet ./... && gofmt -l . && go test ./...
cd arbiter && npm test
```

`go test ./...` on a clean checkout is the whole story: no network, no Docker,
no sleeping, and nothing to install beyond the one module dependency. It runs in
seconds. If a change makes any of that untrue, the change is in the wrong place.

## The constraints

**Two dependencies in the Go module, and both are deliberate.** `go.mod` requires a
YAML parser (`gopkg.in/yaml.v3`) and the AWS SDK (`aws-sdk-go-v2`, needed only
for the optional DynamoDB arbiter backend). That scarcity is deliberate and
worth preserving: this component can stop every service's connector, so every
dependency added to it is code that can take the whole fleet down. The two
places a dependency would ordinarily appear were both avoidable — the Docker
Engine API over a unix socket is plain `net/http`, and `testing` plus
`reflect.DeepEqual` is enough for assertions. Config is the one place a
hand-rolled parser would be worse than a dependency, so YAML gets the
exemption; so does the SDK for the second backend, though it is built into
every binary regardless of which backend a config selects.

**`internal/decide` stays pure.** It is the decision policy as a function of
plain data: no I/O, no clock, no subprocesses, no network. `Test_decide_is_pure`
parses `decide.go`'s own imports and fails on anything outside `fmt` and
`strings`, because an `os` or `time` import would otherwise compile, pass every
other test, and quietly move a decision outside the tested seam. Everything
effectful is an *action returned from* the decision, never a side effect
scattered through the logic — that is what makes `--dry-run` mean "do not
perform the actions" rather than an audit of every place one is called.

If a test of the policy needs a mock, the seam is in the wrong place.

**Time is injected everywhere.** The `Clock` interface (monotonic now, wall now,
sleep) is the supervisor's only source of time, and the tests drive fake time. A
test that sleeps is a defect, not a slow test.

**Every outbound call carries a hard deadline, and a test enforces it.** A
"hung" supervisor is a missing timeout on the arbiter HTTP client or the Docker
socket client. The bug class is eliminated by construction, which is why nothing
here observes conch for signs of life.

**`--dry-run` must not perform actions.** It logs every decision and the exact actions it
would take, executes none, and sends the arbiter only `read` ops — never `renew`
or `release`, enforced by a test — so a dry run pointed at a production arbiter
cannot claim a lease out from under the real active host. It is still a batch, so
the arbiter records the host's presence: the guarantee is no claim, not no write.
Any new action must honour both halves of that.

**Services are configuration, not code.** Adding a service or a hostname is an
edit to a config file. A PR that adds a code path keyed on a particular
application's name is going the wrong way.

**No real infrastructure in the repo.** The example configs are templates:
`web`, `blog`, `host-a`, `example.com`, `<your-registry>/...`,
`<your-worker>.workers.dev`. Please keep your own hostnames, addresses, account
IDs and container names out of commits.

## Tests

Prefer a test that reads as a sentence about behaviour — the existing names do,
and they are the fastest documentation of what the policy actually guarantees.
The action layer is tested with recording fakes rather than mocks: tests assert
"conch did exactly these Docker operations, in this order", and a scripted
transport puts the *real* client under test rather than a stub of it.

Be aware of the limit, though: fixtures are not fault injection. Two real bugs
here surfaced only by running the binary against real Docker while the entire
suite was green. Deliberate fault-injection testing is done privately, against a
real two-host fleet, not in CI; if a change touches the action layer, say in the
PR what you ran it against.

## Commits

Imperative, sentence case, no prefix. Explain *why* in the body — this codebase
is unusually comment-dense on purpose, because the reader is often someone
debugging a failover at 3am.
