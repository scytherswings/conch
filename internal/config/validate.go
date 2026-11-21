package config

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// maxBatchServices mirrors the arbiter's own MAX_SERVICES (also enforced,
// independently, at supervisor.New — defense in depth once a host actually
// boots). Exceeding it fails the whole batch request, not one service, so a
// config with more services than this must be caught here, in CI, not
// discovered as what looks like a permanent arbiter outage at boot.
const maxBatchServices = 64

// statusReservedKey is the one lease key a service may not use.
// supervisor.Status() is keyed by lease key and carries peer presence at the
// same top level (its own comment says why), so a service leasing this name
// would collide with it — silently, in the one output an operator reads at
// 3am. Refusing it in CI is cheaper than either half of that ambiguity.
const statusReservedKey = "peers"

// ttlMin and ttlMax mirror the arbiter's own TTL_MIN/TTL_MAX
// (arbiter/src/lease.js), which silently clamps any ttl outside this range
// to the nearest bound. Left unchecked here, a config could claim a ttl the
// arbiter never actually grants, and every hysteresis computation derived
// from timing.ttl would be computed against a number the lease doesn't have.
const ttlMin, ttlMax = 10, 300

// dynamoRenewChunkSize mirrors the renew batch size the dynamo client sends
// per poll (Task 6) — one UpdateItem per chunk of this many services, not one
// per service, so the capacity guard below must chunk the same way or it
// overstates every poll that covers more than one chunk's worth of services.
const dynamoRenewChunkSize = 20

// dynamoCapacityBudget is 80% of the DynamoDB free tier's 25
// read/write-capacity-units-per-second, left as headroom for burst capacity
// and any other table sharing the account. Projected steady-state usage above
// it is refused here, in CI, rather than discovered as on-call throttling
// once the table is already live. The formula below is steady-state only —
// a contended or failover poll costs up to ~2x the WCU (the main pass's
// renew chunks plus its per-service claim/takeover/release writes, then one
// retry round of renew chunks), and one strongly consistent read billed at
// the full RCU rather than the eventually-consistent half — which is
// exactly the burst this 80% ceiling, not 100%, is headroom for.
const dynamoCapacityBudget = 20

// Validate collects every semantic fault in cfg (spec §6/§11). It never
// stops at the first problem: an operator fixing a config one fault at a
// time and re-running validate would otherwise chase faults one at a time
// too.
func Validate(c Config) []string {
	var faults []string

	if strings.TrimSpace(c.Host) == "" {
		faults = append(faults, "host: must be set, or the arbiter cannot tell which host is reporting")
	}
	if c.Arbiter.OnLoss != "freeze" && c.Arbiter.OnLoss != "stop" {
		faults = append(faults, fmt.Sprintf(
			"arbiter.on_loss: %q is neither freeze nor stop, or arbiter-loss behavior is undefined", c.Arbiter.OnLoss))
	}
	if c.Timing.Interval <= 0 {
		faults = append(faults, fmt.Sprintf(
			"timing.interval: %g must be positive, or RunOnce spins in a tight loop burning the free-tier meters",
			c.Timing.Interval))
	}
	if c.Timing.DemoteAfter < 1 {
		faults = append(faults, fmt.Sprintf(
			"timing.demote_after: %d must be at least 1, or a single missed check demotes with no hysteresis at all",
			c.Timing.DemoteAfter))
	}
	if c.Timing.PromoteAfter < 1 {
		faults = append(faults, fmt.Sprintf(
			"timing.promote_after: %d must be at least 1, or a standby claims after zero consecutive good checks",
			c.Timing.PromoteAfter))
	}
	if c.Timing.PromoteAfter <= c.Timing.DemoteAfter {
		faults = append(faults, fmt.Sprintf(
			"timing.promote_after: %d must exceed demote_after %d, or promotion hysteresis never engages",
			c.Timing.PromoteAfter, c.Timing.DemoteAfter))
	}
	faults = append(faults, validateBackend(c)...)

	if c.Updates.Interval != "" {
		d, err := time.ParseDuration(c.Updates.Interval)
		switch {
		case err != nil:
			faults = append(faults, fmt.Sprintf(
				"updates.interval: %q is not a Go duration (use e.g. 24h, or 0 to disable the version check)",
				c.Updates.Interval))
		case d < 0:
			faults = append(faults, fmt.Sprintf(
				"updates.interval: %q is negative — a negative interval ticks instantly and is never what an operator means; use 0 to disable the version check",
				c.Updates.Interval))
		}
	}

	leaseKeyCount := map[string]int{}
	connectorCount := map[string]int{}
	serviceContainer := map[string]bool{}
	// accessoryCount counts each accessory name once per service that lists
	// it: a within-service repeat is its own fault below, and letting it also
	// inflate this count would misreport a private typo as a cross-service
	// ownership fight. Warm accessories are counted in the SAME map, because
	// the ownership question is about the container, not about which list
	// names it — one container, one reconcile, whichever list it came from.
	accessoryCount := map[string]int{}
	// isRegular and isWarm are the two lists kept apart for exactly one
	// question: whether a container is claimed by both kinds of reconcile at
	// once. That overlap is not a duplicate, it is a fight (see below), and it
	// deserves to be told apart from one container listed twice as the same
	// kind.
	isRegular := map[string]bool{}
	isWarm := map[string]bool{}
	for _, s := range c.Services {
		leaseKeyCount[s.Key()]++
		if s.Connector.Container != "" {
			connectorCount[s.Connector.Container]++
		}
		if s.Container != "" {
			serviceContainer[s.Container] = true
		}
		counted := map[string]bool{}
		for _, a := range s.Accessories {
			if a == "" {
				continue
			}
			isRegular[a] = true
			if counted[a] {
				continue
			}
			counted[a] = true
			accessoryCount[a]++
		}
		for _, a := range s.WarmAccessories {
			if a == "" {
				continue
			}
			isWarm[a] = true
			if counted[a] {
				continue
			}
			counted[a] = true
			accessoryCount[a]++
		}
	}
	reportedLeaseKey := map[string]bool{}
	reportedConnector := map[string]bool{}
	reportedAccessory := map[string]bool{}

	// The warm/lease-following overlap, reported here rather than from either
	// per-service loop below because it is a fact about a CONTAINER and both
	// loops would otherwise report it — and reported ahead of them, latched,
	// so the generic duplicate fault stays silent for a name this one already
	// explained. Same service or two services makes no difference: it is the
	// two reconciles that fight, not the two services.
	for _, name := range sortedKeys(isWarm) {
		if !isRegular[name] {
			continue
		}
		reportedAccessory[name] = true
		faults = append(faults, fmt.Sprintf(
			"services: %q is a warm accessory in one place and an ordinary accessory in another, or the warm reconcile starts it on every host while the accessory reconcile stops it wherever that service does not serve (one owner per container)",
			name))
	}

	// One set of ownership rules over both accessory lists of every service:
	// the maps are the whole config's, and the latch is shared, so a container
	// claimed twice is explained once however many lists it turns up in.
	rules := accessoryRules{
		serviceContainer: serviceContainer,
		connectorCount:   connectorCount,
		accessoryCount:   accessoryCount,
		reported:         reportedAccessory,
	}

	for i, s := range c.Services {
		if strings.TrimSpace(s.Name) == "" {
			faults = append(faults, fmt.Sprintf(
				"services[%d].name: must be set, or the service has no identity to lease", i))
		}
		if strings.TrimSpace(s.Container) == "" {
			faults = append(faults, fmt.Sprintf(
				"services[%d].container: must be set, or Inspect(\"\") errors and the service container can never be started or stopped", i))
		} else if f := containerPrefixFault(fmt.Sprintf("services[%d].container", i), s.Container); f != "" {
			faults = append(faults, f)
		}
		if strings.TrimSpace(s.Connector.Container) == "" {
			faults = append(faults, fmt.Sprintf(
				"services[%d].connector.container: must be set, or Inspect(\"\") errors and the service is ineligible to ever promote", i))
		} else if strings.Contains(s.Connector.Container, "*") {
			// Unlike the service container, prefix matching is not supported
			// here at all — connector.container names a Kamal ACCESSORY
			// (cloudflared or similar), which Kamal never versions the way it
			// versions an app's own container, so there is nothing for a "*"
			// to legitimately mean. Any asterisk, anywhere in the name, is a
			// fault rather than being resolved literally against a container
			// that will never exist.
			faults = append(faults, fmt.Sprintf(
				"services[%d].connector.container: %q must not contain *, or prefix matching is not supported for connector containers (they are stable-named Kamal accessories)",
				i, s.Connector.Container))
		}

		// Both accessory lists: exact names, one owner per container. The
		// ownership faults exist because two things reconciling one container
		// fight — one starts it wherever its service serves while the other
		// stops it wherever its own does not, once per interval, forever, and
		// which wins each round depends on executor timing nobody can read off
		// the config.
		faults = append(faults, rules.faults(
			fmt.Sprintf("services[%d].accessories", i), "accessory", "accessories", s.Accessories)...)
		faults = append(faults, rules.faults(
			fmt.Sprintf("services[%d].warm_accessories", i), "warm accessory", "warm accessories", s.WarmAccessories)...)

		threshold := c.Timing.Interval*float64(c.Timing.DemoteAfter) + c.StopDeadline(s)
		if c.Timing.TTL <= threshold {
			faults = append(faults, fmt.Sprintf(
				"timing.ttl: %g must exceed interval×demote_after + stop deadline for %s (%g×%d + %g = %g), or hysteresis is dead code",
				c.Timing.TTL, s.Name, c.Timing.Interval, c.Timing.DemoteAfter, c.StopDeadline(s), threshold))
		}

		key := s.Key()
		if key == statusReservedKey {
			field := "name"
			if s.LeaseKey != "" {
				field = "lease_key"
			}
			faults = append(faults, fmt.Sprintf(
				"services[%d].%s: %q is reserved, or this service's entry and conch status's own peer presence map collide under one key",
				i, field, statusReservedKey))
		}
		if leaseKeyCount[key] > 1 && !reportedLeaseKey[key] {
			reportedLeaseKey[key] = true
			faults = append(faults, fmt.Sprintf(
				"services: lease_key %q is used by more than one service, or the arbiter cannot tell them apart", key))
		}
		if s.Connector.Container != "" && connectorCount[s.Connector.Container] > 1 && !reportedConnector[s.Connector.Container] {
			reportedConnector[s.Connector.Container] = true
			faults = append(faults, fmt.Sprintf(
				"services: connector.container %q is used by more than one service, or reconcile cannot tell which service owns it",
				s.Connector.Container))
		}

		if !c.HostNetwork {
			if isLoopback(s.Health.URL) {
				faults = append(faults, fmt.Sprintf(
					"services[%d].health.url: %q is a loopback address without host_network: true, or the connector container can't reach it over the docker network",
					i, s.Health.URL))
			}
			if isLoopback(s.Connector.ReadyURL) {
				faults = append(faults, fmt.Sprintf(
					"services[%d].connector.ready_url: %q is a loopback address without host_network: true, or the connector container can't reach it over the docker network",
					i, s.Connector.ReadyURL))
			}
		}
		if f := healthPlaceholderFault(fmt.Sprintf("services[%d].health.url", i), s.Health.URL); f != "" {
			faults = append(faults, f)
		}
		if s.Health.Host != "" && s.Health.URL == "" {
			faults = append(faults, fmt.Sprintf(
				"services[%d].health.host: %q is set but health.url is empty, or there is no probe request for it to attach to",
				i, s.Health.Host))
		}

		faults = append(faults, validateHooks(i, "promote", s.Hooks.Promote)...)
		faults = append(faults, validateHooks(i, "evacuate", s.Hooks.Evacuate)...)

		if s.Evacuation.AutoReclaimAfter != "" {
			if _, err := time.ParseDuration(s.Evacuation.AutoReclaimAfter); err != nil {
				faults = append(faults, fmt.Sprintf(
					"services[%d].evacuation.auto_reclaim_after: %q is not a valid duration, or auto-reclaim can never fire",
					i, s.Evacuation.AutoReclaimAfter))
			}
		}
	}

	return faults
}

// accessoryRules is the one-owner-per-container check both accessory lists
// get. It is one function over both lists rather than two copies because the
// rules ARE the same rules — a warm accessory is reconciled on a different
// schedule, not by different ownership arithmetic — and the only thing that
// differs is which field an operator has to go and edit. The maps are the
// whole-config counts built in Validate; reported is Validate's own latch, so
// a container claimed by two services is told about once however many lists
// it appears in.
type accessoryRules struct {
	serviceContainer map[string]bool
	connectorCount   map[string]int
	accessoryCount   map[string]int
	reported         map[string]bool
}

// faults reads one service's list. field is the YAML path up to the index
// (e.g. "services[0].warm_accessories"); kind and kinds are what to call one
// entry and the list in the fault text, since "accessories are stable-named"
// and "warm accessories are stable-named" are the same ruling about two
// different fields an operator can only fix by name.
func (r accessoryRules) faults(field, kind, kinds string, names []string) []string {
	var faults []string
	seen := map[string]bool{}
	for j, a := range names {
		at := fmt.Sprintf("%s[%d]", field, j)
		if strings.TrimSpace(a) == "" {
			faults = append(faults, fmt.Sprintf(
				"%s: must not be empty, or Inspect(\"\") errors on every check and the accessory can never be reconciled", at))
			continue
		}
		if strings.Contains(a, "*") {
			// The connector.container ruling, verbatim: an accessory is
			// hand-created or Kamal-booted, never versioned the way Kamal
			// versions an app's own container, so there is nothing for a
			// "*" to legitimately mean.
			faults = append(faults, fmt.Sprintf(
				"%s: %q must not contain *, or prefix matching is not supported for %s (they are stable-named containers)",
				at, a, kinds))
			continue
		}
		if seen[a] {
			faults = append(faults, fmt.Sprintf(
				"%s: %q is listed twice for this service, or every accessory action would act on it twice", at, a))
			continue
		}
		seen[a] = true
		switch {
		case r.serviceContainer[a]:
			faults = append(faults, fmt.Sprintf(
				"%s: %q is also a service's container, or two reconciles fight over one container (one owner per container)",
				at, a))
		case r.connectorCount[a] > 0:
			faults = append(faults, fmt.Sprintf(
				"%s: %q is also a connector container, or two reconciles fight over one container (one owner per container)",
				at, a))
		case r.accessoryCount[a] > 1:
			if !r.reported[a] {
				r.reported[a] = true
				faults = append(faults, fmt.Sprintf(
					"services: %s %q belongs to more than one service, or two services' reconciles fight over one container (one owner per container)",
					kind, a))
			}
		}
	}
	return faults
}

// sortedKeys is deterministic iteration over a set, so a config with two
// faulty containers reports them in the same order every run — an operator
// diffing two validate runs is reading for what CHANGED.
func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// validateBackend reports every fault specific to which arbiter backend c
// selects. The two backends' fields are mutually exclusive (ArbiterConfig's
// doc comment), so this is the one place that branches on Backend instead of
// checking every backend-specific field unconditionally.
func validateBackend(c Config) []string {
	var faults []string
	switch c.Arbiter.Backend {
	case "cloudflare":
		if strings.TrimSpace(c.Arbiter.URL) == "" {
			faults = append(faults, "arbiter.url: must be set, or conch has nowhere to send leases")
		}
		if c.Timing.TTL < ttlMin || c.Timing.TTL > ttlMax {
			faults = append(faults, fmt.Sprintf(
				"timing.ttl: %g is outside [%d, %d], or the arbiter clamps the granted lease to that range while every hysteresis computation here still uses the unclamped value",
				c.Timing.TTL, ttlMin, ttlMax))
		}
		if len(c.Services) > maxBatchServices {
			faults = append(faults, fmt.Sprintf(
				"services: %d exceeds the arbiter's %d-service batch cap, or conch's single batch request to the arbiter fails outright",
				len(c.Services), maxBatchServices))
		}
	case "dynamo":
		if strings.TrimSpace(c.Arbiter.Table) == "" {
			faults = append(faults, "arbiter.table: must be set, or the dynamo backend has no table to read or write leases in")
		}
		if strings.TrimSpace(c.Arbiter.Region) == "" {
			faults = append(faults, "arbiter.region: must be set, or the AWS SDK has no region to sign requests for")
		}
		if strings.TrimSpace(c.Arbiter.CredentialsFile) == "" {
			faults = append(faults, "arbiter.credentials_file: must be set, or the dynamo backend has no credentials to authenticate with")
		}
		if strings.TrimSpace(c.Arbiter.URL) != "" {
			faults = append(faults, "arbiter.url: must be blank for the dynamo backend, or a config carrying both backends' fields is confused about which one it targets")
		}
		if strings.TrimSpace(c.Arbiter.SecretEnv) != "" {
			faults = append(faults, "arbiter.secret_env: must be blank for the dynamo backend, or a config carrying both backends' fields is confused about which one it targets")
		}
		if strings.TrimSpace(c.Arbiter.SecretFile) != "" {
			faults = append(faults, "arbiter.secret_file: must be blank for the dynamo backend, or a config carrying both backends' fields is confused about which one it targets")
		}
		if c.Timing.TTL > ttlMax {
			faults = append(faults, fmt.Sprintf(
				"timing.ttl: %g exceeds the dynamo backend's %d-second cap, or the lease item's ttl attribute never matches what conch's hysteresis assumes",
				c.Timing.TTL, ttlMax))
		}
	default:
		faults = append(faults, fmt.Sprintf(
			"arbiter.backend: %q is neither cloudflare nor dynamo, or conch has no arbiter client to construct", c.Arbiter.Backend))
	}
	return faults
}

// Warnings collects everything about cfg that conch recommends against but
// will not refuse: this codebase's rule is that the tool is designed to be
// free to run, not limited to it — validation faults are reserved for
// configs that cannot work, and anything that merely costs money (or eats
// into free-tier headroom the operator may have decided to pay past), or
// merely leaves an operator with less visibility than they'd want, is a
// warning the operator is free to ignore. Both `conch validate` and
// `conch run` print these and proceed. It composes every independent
// warning source rather than picking one: the backend-specific capacity
// guard and the backend-agnostic dark-webhooks check have nothing to do
// with each other, and a config can trip either, both, or neither.
func Warnings(c Config) []string {
	var warnings []string
	if c.Arbiter.Backend == "dynamo" {
		warnings = append(warnings, dynamoCapacityWarnings(c)...)
	}
	warnings = append(warnings, webhookWarnings(c)...)
	return warnings
}

// webhookWarnings warns when every WebhookConfig field is empty. Webhooks
// are the one channel designed to carry a lease transition between hosts
// (spec §11) — the RCA this warning exists for: production's configs had no
// webhooks: block at all, and conch neither validated nor warned about it,
// so a promotion or demotion showed up nowhere but the host's own log.
// Backend-agnostic on purpose: which arbiter backend a host uses says
// nothing about whether anyone downstream can see its lease transitions.
func webhookWarnings(c Config) []string {
	w := c.Webhooks
	if strings.TrimSpace(w.Promote) != "" ||
		strings.TrimSpace(w.Demote) != "" ||
		strings.TrimSpace(w.Evacuate) != "" ||
		strings.TrimSpace(w.Reclaim) != "" ||
		strings.TrimSpace(w.Alert) != "" {
		return nil
	}
	return []string{"warning: webhooks: no receivers configured — lease transitions are only visible in this host's own log"}
}

// dynamoCapacityWarnings warns when the dynamo backend's projected
// steady-state throughput exceeds dynamoCapacityBudget read/write capacity
// units per second — 80% of the DynamoDB free tier's 25/s, left as headroom
// (spec §5). Every poll interval issues one UpdateItem per
// dynamoRenewChunkSize services (Task 6's renew chunk size), from each of 2
// hosts; itemKB rounds a chunk's item size up to the nearest KB (DynamoDB
// bills whole KB units), and an eventually-consistent read of an item <=4KB
// costs only half a read-capacity-unit (a strongly-consistent read of the
// same item costs a full one).
func dynamoCapacityWarnings(c Config) []string {
	n := float64(len(c.Services))
	itemKB := math.Ceil((n*120 + 2*100) / 1024)
	chunks := math.Ceil(n / dynamoRenewChunkSize)
	wcuPerSec := 2 * (1 / c.Timing.Interval) * chunks * itemKB
	rcuPerSec := 2 * (1 / c.Timing.Interval) * math.Ceil(itemKB/4) * 0.5
	if wcuPerSec <= dynamoCapacityBudget && rcuPerSec <= dynamoCapacityBudget {
		return nil
	}
	return []string{fmt.Sprintf(
		"warning: arbiter: projected dynamo usage (%.1f WCU/s, %.1f RCU/s) exceeds 80%% of the free tier's 25/s provisioned capacity — fine if you have provisioned (and are paying for) more; on the free 25/25 the table throttles under load and leases stop renewing until the next poll",
		wcuPerSec, rcuPerSec)}
}

func validateHooks(serviceIndex int, phase string, hooks []HookSpec) []string {
	var faults []string
	for j, h := range hooks {
		if strings.TrimSpace(h.Image) == "" {
			faults = append(faults, fmt.Sprintf(
				"services[%d].hooks.%s[%d]: image must be set, or the hook has nothing to run",
				serviceIndex, phase, j))
		}
		if len(h.Argv) == 0 {
			faults = append(faults, fmt.Sprintf(
				"services[%d].hooks.%s[%d]: argv must be set, or the hook container has no command",
				serviceIndex, phase, j))
		}
		// Negative is never a default's doing (applyServiceDefaults only
		// fills in Timeout == 0, on purpose) — a negative value only gets
		// here from the operator's own YAML, so it's reported rather than
		// silently folded into the same 300s default a bare omission gets.
		if h.Timeout < 0 {
			faults = append(faults, fmt.Sprintf(
				"services[%d].hooks.%s[%d].timeout: %g must not be negative, or the hook's wait deadline is nonsensical",
				serviceIndex, phase, j, h.Timeout))
		}
		// {container} in argv: promote and evacuate get opposite rulings,
		// because they get opposite runtime treatment (executor.go's
		// substitutePromoteArgv and startEvacuateHooks). Promote hooks DO
		// substitute the exact literal token, so only its near-miss casings
		// are faults there (hookArgvPlaceholderFault below, phase=="promote").
		// Evacuate hooks never resolve a container at all, so every
		// placeholder-shaped token, "{container}" included, is a fault there.
		for k, a := range h.Argv {
			field := fmt.Sprintf("services[%d].hooks.%s[%d].argv[%d]", serviceIndex, phase, j, k)
			if f := hookArgvPlaceholderFault(field, phase, a); f != "" {
				faults = append(faults, f)
			}
		}
	}
	return faults
}

// containerPrefixFault reports a fault if value's "*" is used any way other
// than as a trailing prefix match: mid-string (only a trailing "*" is
// supported — one anywhere else looks like a glob this code doesn't
// implement, and treating it as a literal character would be worse than
// refusing it) or bare (a "*" with nothing before it has no literal prefix
// to match against, and would resolve to whichever container the daemon
// happens to list first). A value with no "*" at all is always fine.
func containerPrefixFault(field, value string) string {
	idx := strings.IndexByte(value, '*')
	if idx == -1 {
		return ""
	}
	if idx != len(value)-1 {
		return fmt.Sprintf(
			"%s: %q has a * that is not the final character, or prefix matching only supports a trailing *",
			field, value)
	}
	if idx == 0 {
		return fmt.Sprintf(
			"%s: %q is a bare *, or prefix matching needs at least one literal character before it",
			field, value)
	}
	return ""
}

// containerPlaceholderPattern matches every brace- or dollar-brace-shaped
// token in a string: an optional leading "$", one or more "{", anything but
// "}", then one or more "}". It exists to catch every near-miss of health.
// url's "{container}" placeholder — {Container} (wrong case), ${container}
// (an extra $, as if it were shell or template-engine syntax), {{container}}
// (doubled braces) — each of which Validate must reject: supervisor.readings
// substitutes ONLY the exact literal "{container}" via strings.ReplaceAll,
// so anything else either never substitutes at all (leaving a URL that can
// never resolve) or, for the doubled-brace case, substitutes the inner
// "{container}" and leaves stray literal braces around the result.
//
// Known sharp edge: hookArgvPlaceholderFault reuses this same pattern over
// hook Argv, and Argv is free-form command-line text in a way health.url
// never was — a legitimate argument like docker's own
// `--format={{.State.Health}}`, or a literal JSON blob, is brace-shaped and
// would match this pattern and fault, even though it has nothing to do with
// the {container} placeholder. No shipped config hits this today, and
// widening the pattern to tell the two apart is a behavior change out of
// scope for this fix. The workaround for an operator who hits it: put the
// brace-heavy argument in a script the hook invokes (`argv: ["run.sh"]`)
// rather than inline in argv, where this pattern never sees it.
var containerPlaceholderPattern = regexp.MustCompile(`\$?\{+[^}]*\}+`)

// healthPlaceholderFault reports a fault if url contains any
// placeholder-shaped token that is not EXACTLY "{container}". A url with no
// such token at all (an ordinary exact-name health check) is always fine.
func healthPlaceholderFault(field, url string) string {
	var bad []string
	for _, m := range containerPlaceholderPattern.FindAllString(url, -1) {
		if m != "{container}" {
			bad = append(bad, fmt.Sprintf("%q", m))
		}
	}
	if len(bad) == 0 {
		return ""
	}
	return fmt.Sprintf(
		"%s: %q contains %s, which is not exactly \"{container}\" and will never substitute at probe time",
		field, url, strings.Join(bad, ", "))
}

// hookArgvPlaceholderFault is healthPlaceholderFault's ruling applied to one
// promote- or evacuate-hook argv token, with evacuate's ruling widened: the
// two phases get opposite runtime treatment (executor.go's
// substitutePromoteArgv only ever runs for promote — startEvacuateHooks calls
// hooks.Runner.Run with each evacuate hook's Argv completely untouched,
// because an evacuation runs off this service's own record, never through
// resolveContainer, so there is no resolved name in scope to give it), so
// they get opposite validation:
//
//   - promote: the exact literal "{container}" DOES substitute at hook-run
//     time, so only a placeholder-shaped token that ISN'T exactly that is a
//     fault — the same near-miss guard healthPlaceholderFault applies to
//     health.url, and for the identical reason (it would silently never
//     substitute, and the hook runs with the literal typo as an argument).
//   - evacuate: NOTHING here ever substitutes, so every placeholder-shaped
//     token is a fault, "{container}" included — left unrefused, it would
//     reach docker as the literal eleven-character string "{container}",
//     which is very likely not a container this host has.
func hookArgvPlaceholderFault(field, phase, arg string) string {
	matches := containerPlaceholderPattern.FindAllString(arg, -1)
	if phase == "promote" {
		var bad []string
		for _, m := range matches {
			if m != "{container}" {
				bad = append(bad, fmt.Sprintf("%q", m))
			}
		}
		if len(bad) == 0 {
			return ""
		}
		return fmt.Sprintf(
			"%s: %q contains %s, which is not exactly \"{container}\" and will never substitute when the hook runs",
			field, arg, strings.Join(bad, ", "))
	}
	if len(matches) == 0 {
		return ""
	}
	quoted := make([]string, len(matches))
	for i, m := range matches {
		quoted[i] = fmt.Sprintf("%q", m)
	}
	return fmt.Sprintf(
		"%s: %q contains %s, or evacuate hooks never resolve a container name (they run off the check path with none in scope) and the literal text reaches docker",
		field, arg, strings.Join(quoted, ", "))
}

// isLoopback reports whether url points at a loopback address — a health or
// ready-check URL that only resolves inside the conch container's own
// network namespace, not the docker-network namespace the target actually
// runs in (spec §11's "namespace honesty" rule).
func isLoopback(url string) bool {
	return strings.Contains(url, "127.0.0.1") || strings.Contains(url, "localhost")
}
