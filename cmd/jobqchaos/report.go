package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// report is everything one invocation found, as written to results.json.
type report struct {
	Started     time.Time    `json:"started"`
	Finished    time.Time    `json:"finished"`
	Seed        int64        `json:"seed"`
	Nodes       int          `json:"nodes"`
	Runs        int          `json:"runs_per_fault"`
	HoldMs      float64      `json:"hold_ms"`
	SettleMs    float64      `json:"settle_ms"`
	Producers   int          `json:"producers"`
	Consumers   int          `json:"consumers"`
	PaceMs      float64      `json:"pace_ms"`
	Interrupted bool         `json:"interrupted,omitempty"`
	Cells       []cellResult `json:"faults"`
}

// ok reports whether every fault ran, was seen every time, and left the
// contract standing.
func (r report) ok() bool {
	if r.Interrupted || len(r.Cells) == 0 {
		return false
	}
	for _, c := range r.Cells {
		if c.Error != "" || len(c.Contract.Violations) > 0 || len(c.Runs) < r.Runs {
			return false
		}
		for _, run := range c.Runs {
			if !run.Seen {
				return false
			}
		}
	}
	return true
}

// spread is the median, 99th percentile and maximum of the non-negative
// values of one figure across a fault's runs. n is how many there were.
type spread struct {
	n             int
	p50, p99, max float64
}

func spreadOf(runs []runResult, pick func(runResult) float64) spread {
	var ds []time.Duration
	for _, r := range runs {
		if v := pick(r); v >= 0 && r.Error == "" {
			ds = append(ds, time.Duration(v*float64(time.Millisecond)))
		}
	}
	return spread{n: len(ds), p50: ms(percentile(ds, 50)), p99: ms(percentile(ds, 99)), max: ms(percentile(ds, 100))}
}

func (s spread) String() string {
	if s.n == 0 {
		return "none"
	}
	return fmt.Sprintf("%.0f / %.0f / %.0f", s.p50, s.p99, s.max)
}

func (r report) markdown() string {
	var b strings.Builder
	total := contract{}
	fmt.Fprintf(&b, "%d nodes, %d runs per fault, each held %s; %d producers, %d consumers. Seed %d.\n\n",
		r.Nodes, r.Runs, time.Duration(r.HoldMs*float64(time.Millisecond)), r.Producers, r.Consumers, r.Seed)

	b.WriteString("| Fault | Runs | Seen | Acknowledged enqueues | Lost | Ghosts | Duplicates |\n")
	b.WriteString("|---|---|---|---|---|---|---|\n")
	for _, c := range r.Cells {
		seen := 0
		for _, run := range c.Runs {
			if run.Seen {
				seen++
			}
		}
		k := c.Contract
		fmt.Fprintf(&b, "| %s | %d | %d | %d | %d | %d | %d |\n", c.Fault, len(c.Runs), seen, k.Acknowledged, k.Lost, k.Ghosts, k.Duplicates)
		total.Acknowledged += k.Acknowledged
		total.Lost += k.Lost
		total.Ghosts += k.Ghosts
		total.Duplicates += k.Duplicates
	}
	fmt.Fprintf(&b, "| **all** | | | **%d** | **%d** | **%d** | **%d** |\n\n", total.Acknowledged, total.Lost, total.Ghosts, total.Duplicates)

	b.WriteString("Milliseconds, as median / 99th percentile / maximum across the runs; below 100 runs the 99th percentile is the maximum. ")
	b.WriteString("The first three figures are timed from just before the fault was injected, so each includes the time injecting it took.\n\n")
	b.WriteString("| Fault | First write after the fault | Longest stall | New leader seen (runs) | Whole again after undo | Write p50, before -> during | Injecting took (median) |\n")
	b.WriteString("|---|---|---|---|---|---|---|\n")
	for _, c := range r.Cells {
		inject := spreadOf(c.Runs, func(r runResult) float64 { return r.InjectMs })
		first := spreadOf(c.Runs, func(r runResult) float64 { return r.FirstWriteMs })
		stall := spreadOf(c.Runs, func(r runResult) float64 { return r.LongestStallMs })
		lead := spreadOf(c.Runs, func(r runResult) float64 { return r.NewLeaderMs })
		whole := spreadOf(c.Runs, func(r runResult) float64 { return r.WholeAgainMs })
		before := spreadOf(c.Runs, func(r runResult) float64 { return r.BaselineP50Ms })
		during := spreadOf(c.Runs, func(r runResult) float64 { return r.FaultP50Ms })
		leadCell := "none"
		if lead.n > 0 {
			leadCell = fmt.Sprintf("%s (%d of %d)", lead, lead.n, len(c.Runs))
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %.1f -> %.1f | %.0f |\n", c.Fault, first, stall, leadCell, whole, before.p50, during.p50, inject.p50)
	}

	var problems []string
	for _, c := range r.Cells {
		if c.Error != "" {
			problems = append(problems, fmt.Sprintf("%s: %s", c.Fault, c.Error))
		}
		for _, run := range c.Runs {
			if !run.Seen && run.Error == "" {
				problems = append(problems, fmt.Sprintf("%s run %d: fault not seen: %s", c.Fault, run.Run, run.Evidence))
			}
		}
		for _, v := range c.Contract.Violations {
			problems = append(problems, fmt.Sprintf("%s: %s", c.Fault, v))
		}
	}
	if r.Interrupted {
		problems = append(problems, "interrupted before every fault had its runs")
	}
	b.WriteString("\n")
	if len(problems) == 0 {
		b.WriteString("Contract held: nothing acknowledged was lost, nothing acked came back, and every fault was seen to take effect.\n")
	} else {
		b.WriteString("**FAILED:**\n\n")
		for _, p := range problems {
			fmt.Fprintf(&b, "- %s\n", p)
		}
	}
	return b.String()
}

// write puts results.json and results.md in dir.
func (r report) write(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	js, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "results.json"), append(js, '\n'), 0o644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "results.md"), []byte(r.markdown()), 0o644)
}
