package supervisor

import (
	"sort"

	"github.com/scytherswings/conch/internal/config"
	"github.com/scytherswings/conch/internal/dockerapi"
	"github.com/scytherswings/conch/internal/hooks"
)

// The conch.service label sweep (spec §11's "reconcile stops any labeled
// connector the config no longer accounts for"). The deploy templates label
// every connector container conch.service: <name>; this is the half that
// consumes it, so a service that was renamed or deleted from the config
// cannot leave its old connector serving traffic nothing supervises.
//
// It runs once per check, at host level rather than per service, because it
// is a question about the HOST — "is anything here wearing a label this
// config does not account for" — not about any one service. One bounded
// listing per check answers it for every container at once.
//
// # The safety argument
//
// The sweep only ever STOPS containers. It never starts one, never creates
// one, never removes one, and never touches a lease. That asymmetry is the
// whole reason it can be always-on with no config knob: a stop is recoverable
// by a human in one command and cannot produce the failure this system exists
// to prevent (two hosts serving at once), while a start could. Every
// uncertainty is therefore resolved towards stopping LESS — a listing that
// fails reads as "nothing is labeled" (dockerapi.ListWithLabel), and every
// container this sweep cannot positively prove is unaccounted-for is left
// alone.
//
// # What it deliberately does not touch
//
//   - Containers whose label names a service THIS config has. Those belong to
//     that service's own per-check reconcile, which knows whether this host
//     should be serving; the sweep knows only that a label exists.
//   - Containers this config NAMES — any service's connector container or
//     service container — whatever their label says. The label is a string an
//     operator typed and can get wrong (writing the container name where the
//     service name belongs, say); the config's own container fields are what
//     the per-service reconcile acts on every check. Without this, a
//     mislabeled connector on an ACTIVE host would be stopped by the sweep and
//     started again by decide's rule 12, once per interval, silently after the
//     first warning, until the connector's readiness failures demoted the host.
//     That is the one case where "stopping is always safe" is false — not
//     because a stop is dangerous in itself, but because something else is
//     about to undo it.
//   - Containers that are already stopped. There is nothing unsupervised
//     about a container that is not running, and stopping it again would be a
//     daemon call per check forever for no change in the world.
//   - Anything carrying conch.hook. Every hook container the hook runner
//     creates carries BOTH labels (hooks.Run stamps conch.hook=true and
//     conch.service=<service>), so a sweep that ignored the hook label would
//     kill a running restore mid-flight — including one for a service that is
//     configured, since the value it carries is the service name. The hook
//     runner owns those containers, at their own timeouts, with its own
//     orphan sweep at startup (spec §8 rule 4).
//
// And one thing it cannot know: the label alone is what it acts on, so it
// cannot tell a connector from an application container. The label belongs on
// connectors and nothing else, which is what the deploy templates do.

// sweepScope is the synthetic unit key the sweeper's executor logs and alerts
// under. It is not a lease key and never will be — no service is named here,
// which is the entire point of the sweep — so it is written as something no
// config could produce (config.Validate requires a non-empty name, and a lease
// key with spaces would be a very strange one), so that an operator reading a
// log line or an alert payload cannot mistake it for a service of theirs. It
// is what stopContainer's wedged-daemon alert — the one alert on this path
// built from the unit rather than by sweep.go — carries in its "service" key
// for a swept container that survived both stop and kill.
const sweepScope = "<conch.service sweep>"

// defaultConnectorStopTimeout is config's own default for
// connector.stop_timeout (config.go's applyDefaults), which is the budget an
// unsupervised connector gets: its real one left with the config entry that
// was removed or renamed, and this is the value that entry would have had if
// it had never set the knob at all.
const defaultConnectorStopTimeout = 35

// newSweeper builds the executor the sweep dispatches to. It is a full
// executor over a synthetic unit rather than a goroutine of its own so the
// sweep inherits, unchanged, the property spec §5 demands of every stop —
// "stops must not queue behind the loop" — and so a sweep stopping a
// slow-draining connector cannot delay any service's own actions either: it
// is one more parallel pair of hands, not a turn in an existing queue.
//
// The synthetic unit exists only to give that executor a stop timeout and a
// name to log under. It is never registered in s.units and never appears in
// s.order, so nothing iterating services can see it: it has no lease, no
// phase, no hooks and no decisions. The action arms that would read those
// (StopConnector's noteConnectorStopped, Alert's phaseOf) are unreachable
// from applyStops, which is the only path this executor is ever given work
// through.
func newSweeper(s *Supervisor) *executor {
	return newExecutor(s, &unit{
		key: sweepScope,
		svc: config.Service{
			Name:      sweepScope,
			Connector: config.ConnectorConfig{StopTimeout: defaultConnectorStopTimeout},
		},
	})
}

// sweepUnsupervised is one check's sweep: list every container carrying the
// conch.service label at all, work out which of them are running under a label
// value this config does not account for, warn once per streak about each, and
// hand the stops to the sweeper.
//
// resolved is the set of concrete service-container names THIS check resolved
// (RunOnce's prepared.resolvedContainer, one entry per service), which is why
// the sweep runs after the readings loop rather than beside hostFacts: for a
// trailing-* container pattern, the only name the config can be said to name
// is the one this check's resolution actually picked. See namedByConfig.
//
// Called from RunOnce on the loop goroutine, once per check. Like hostFacts
// and reportPrefixOverlap it reads and writes loop-goroutine-only state
// (sweepLogged) with no lock, because RunOnce is not safe for concurrent use
// and nothing else touches it.
func (s *Supervisor) sweepUnsupervised(resolved map[string]bool) {
	listed := s.dk.ListWithLabel(hooks.ServiceLabel)
	if len(listed) == 0 {
		// Nothing is labeled, or the daemon could not be asked — nil answers
		// both (dockerapi.ListWithLabel), and this deliberately does not try
		// to tell them apart. Either way there is nothing to act on, and the
		// streak is dropped so a container that reappears in the listing later
		// is news again. The cost of the conflation is one repeated warning
		// per unsupervised container each time a flapping daemon misses a
		// listing; the alternative is an error return whose only reader would
		// be this branch, for a log line rather than a safety property.
		s.sweepLogged = nil
		return
	}

	accounted := s.accountedServiceLabels()
	seen := make(map[string]bool, len(listed))
	var stops []string
	for _, c := range listed {
		v := s.classify(c, accounted, resolved)
		if v == sweepLeave {
			continue
		}
		seen[c.Name] = true
		if v == sweepStop {
			stops = append(stops, c.Name)
		}
		// One telling per streak of the SAME verdict about the same container
		// (see sweepLogged): a verdict that changes is a different thing to
		// say, and the change from "supervised, wearing an inert label" to
		// "unsupervised, being stopped" is the one an operator most needs to
		// hear — it is reachable without anything on the host changing at all,
		// since a blue/green deploy moves which container a trailing-*
		// pattern resolves to. Latching on the container name alone made that
		// flip stop a container in silence, with no warning and no webhook.
		if s.sweepLogged[c.Name] == v {
			continue
		}
		if s.sweepLogged == nil {
			s.sweepLogged = map[string]verdict{}
		}
		s.sweepLogged[c.Name] = v
		switch v {
		case sweepStop:
			s.reportUnsupervised(c)
		case sweepMislabeled:
			// Named by this config, so not the sweep's to stop — but the
			// label on it says otherwise, and an operator who typed it
			// deserves to hear that it does nothing.
			s.reportMislabeled(c)
		case sweepInertLabel:
			s.reportInertLabel(c)
		}
	}
	// The latch is rebuilt, not accumulated: an entry for a container that is
	// no longer in this check's set — stopped by the last check's sweep,
	// removed, or renamed back into the config — is dropped, so a container
	// that comes back up (`unless-stopped` after a reboot, say) starts a new
	// streak and warns again. It also keeps the map bounded by what is on the
	// host right now rather than by everything that ever passed through it.
	// A container that stays in the set but changes verdict is not deleted
	// here; it was overwritten above, and a flip BACK to a verdict it held
	// before is a new streak of that verdict and says so again, which is the
	// intent: each transition is news, each repetition is not.
	for name := range s.sweepLogged {
		if !seen[name] {
			delete(s.sweepLogged, name)
		}
	}

	// Sorted so a check that stops several containers does it in an order a
	// test and an operator can both predict; the daemon has no opinion.
	sort.Strings(stops)
	s.sweeper.submitStops(stops)
}

// verdict is what one check has to say about one labeled container.
type verdict int

const (
	sweepLeave      verdict = iota // not the sweep's business, and nothing to say
	sweepInertLabel                // the label is present but empty: it names nothing
	sweepMislabeled                // this config names the container; the label names nothing
	sweepStop                      // running, unaccounted for, and nothing else's to stop
)

// classify is the whole discriminating question, and every branch of it errs
// towards leaving the container alone.
func (s *Supervisor) classify(
	c dockerapi.LabeledContainer, accounted map[string]bool, resolved map[string]bool,
) verdict {
	if !c.Running {
		return sweepLeave // a stopped container serves nobody
	}
	if c.Labels[hooks.HookLabel] != "" {
		return sweepLeave // the hook runner's container, at its own timeouts
	}
	if c.Value == "" {
		// The label is present but empty, which names no service at all —
		// overwhelmingly a deploy template whose interpolation produced
		// nothing. That is a mistake in the template, not evidence that this
		// container is unaccounted for, so stopping on it would turn a typo
		// into an outage. It is still said out loud: an empty label is inert,
		// and its author believes it is protecting them.
		return sweepInertLabel
	}
	if accounted[c.Value] {
		return sweepLeave // the labeled service's own reconcile owns it
	}
	if s.namedByConfig(c.Name, resolved) {
		return sweepMislabeled
	}
	return sweepStop
}

// namedByConfig reports whether this container is one the config itself names
// — any service's connector container, any service's own exact container
// name, or a name THIS CHECK resolved for a trailing-* container pattern.
//
// The prefix case is deliberately not a HasPrefix test. A pattern like
// "app-*" matches every container whose name starts with it, including ones
// this host has never supervised, and shielding all of them would hand any
// container an exemption for being named conveniently — a rogue "app-blog-cf"
// labeled for a service this config does not have would be left running under
// a warning that calls it supervised, which is both wrong and quiet. What the
// config actually names, for a pattern, is whatever the pattern resolved to on
// this check (supervisor.go's resolveContainer, threaded in as `resolved`):
// that one name is what this check's reconcile would act on, and it is the
// only name a stop here could fight.
//
// Exact names are checked against the config directly as well as through
// resolved. They are the same answer today — resolveContainer returns an exact
// name unchanged, so it is always in the set — and keeping the direct check
// means this function still has the right answer for a caller that has no
// resolutions to offer.
func (s *Supervisor) namedByConfig(name string, resolved map[string]bool) bool {
	if resolved[name] {
		return true
	}
	for _, svc := range s.cfg.Services {
		if name == svc.Connector.Container {
			return true
		}
		if _, isPrefix := svc.ContainerPrefix(); !isPrefix && name == svc.Container {
			return true
		}
		// Accessories are config-named exactly like the connector — always
		// exact names, never patterns — and the same undo argument applies:
		// on the host where their service is ACTIVE, decide's rule 12 would
		// restart a swept accessory on the next check, forever.
		for _, acc := range svc.Accessories {
			if name == acc {
				return true
			}
		}
		// Warm accessories earn the shield twice over. They are config-named
		// exactly like the others, and the reconcile that would undo a sweep
		// here is not decide's rule 12 but the supervisor's own warm start
		// (supervisor.go's startWarmAccessories), which runs on EVERY host in
		// every phase — so a swept warm accessory would come back on the next
		// check on both hosts at once, once per interval, forever, with the
		// service's database as the rope in that tug of war.
		for _, acc := range svc.WarmAccessories {
			if name == acc {
				return true
			}
		}
	}
	return false
}

// accountedServiceLabels is every label value this config accounts for.
//
// Both the service NAME and its lease KEY are in the set, and the difference
// matters: spec §11 says the deploy templates label connectors with the
// service name (`conch.service: <name>`), while everything else in this
// package identifies a service by svc.Key(), which is the lease_key when one
// is set and the name otherwise. A config that sets lease_key: web-prod on a
// service named web has a correctly labeled, live, supervised connector
// carrying "web" — and a set built from keys alone would stop it. Accepting
// either identifier costs nothing (a collision between one service's name and
// another's lease key is a config that config.Validate would already have
// refused for duplicate keys) and removes an entire class of false positive
// from a sweep whose false positives are outages.
func (s *Supervisor) accountedServiceLabels() map[string]bool {
	accounted := make(map[string]bool, 2*len(s.cfg.Services))
	for _, svc := range s.cfg.Services {
		accounted[svc.Name] = true
		accounted[svc.Key()] = true
	}
	return accounted
}

// reportUnsupervised warns and alerts for one container, called by the sweep
// once per streak of this verdict — the same shape as reportPrefixOverlap and
// logArbiterUnreachable: a connector left behind by a rename sits there for as
// long as it takes a human to notice, and one warning every interval forever
// about a condition that has not changed is how an operator learns to filter
// conch out. A wedged daemon makes that literal: the stop is re-dispatched
// every check and never takes, so an unlatched alert here would be a webhook
// every interval for as long as the daemon stays wedged.
//
// The stop itself is NOT latched — every check re-dispatches it while the
// container is still running, because a stop that failed (a wedged daemon, a
// container that came back) must be retried, and the sweep is a reconcile like
// every other part of this loop. Only the telling is once.
func (s *Supervisor) reportUnsupervised(c dockerapi.LabeledContainer) {
	s.log.Warn("stopping a connector this config does not account for: its conch.service label names a service that is not configured on this host",
		"container", c.Name, "label", c.Value, "host", s.cfg.Host)
	// Deliberately no "service" key in the payload: every other alert in this
	// package uses it for a configured lease key, and the whole point of this
	// one is that the label names something this config has never heard of.
	s.notify("alert", map[string]any{
		"host":      s.cfg.Host,
		"container": c.Name,
		"label":     c.Value,
		"reason":    "unsupervised connector stopped: label names a service this config does not",
	})
}

// reportMislabeled is the other half of the telling, once per streak on the
// same latch: a container this config names, wearing a label value that names
// no service. Nothing is stopped and nothing is broken — the per-service
// reconcile supervises this container by its config entry, not by its label —
// but the label is inert, and the next rename will not sweep what an operator
// expects it to.
//
// A log line and no webhook, deliberately: this is config hygiene a human
// fixes at leisure, not an incident. The one webhook this file sends is for a
// container that was actually stopped.
func (s *Supervisor) reportMislabeled(c dockerapi.LabeledContainer) {
	s.log.Warn("a container this config supervises carries a conch.service label naming no configured service: the label does nothing, fix it to the service name",
		"container", c.Name, "label", c.Value, "host", s.cfg.Host)
}

// reportInertLabel is the empty-value case, on the same once-per-streak latch
// and, like reportMislabeled, a log line and no webhook. An empty
// conch.service label is almost always a deploy template whose interpolation
// produced nothing — `conch.service: ${SERVICE}` with SERVICE unset — and the
// operator who wrote it believes the container is covered by the sweep when it
// is covered by nothing at all. The sweep will not stop it (an empty value is
// evidence of a broken template, not of an unaccounted-for container), so
// saying so is the only way that belief ever gets corrected.
func (s *Supervisor) reportInertLabel(c dockerapi.LabeledContainer) {
	s.log.Warn("empty conch.service label: interpolation failed? the label is inert, and this container is covered by nothing",
		"container", c.Name, "host", s.cfg.Host)
}
