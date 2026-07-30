package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/aleks-drozy/jobq"
)

// formatStatsTable renders topic stats as a simple aligned table, sorted by
// topic name so output is deterministic across runs (a pure, testable
// function: no I/O, easy to pin with a golden string).
func formatStatsTable(byTopic map[string]jobq.Stats) string {
	if len(byTopic) == 0 {
		return "(no topics)\n"
	}
	names := make([]string, 0, len(byTopic))
	for name := range byTopic {
		names = append(names, name)
	}
	sort.Strings(names)

	headers := []string{"TOPIC", "ENQUEUED", "READY", "INFLIGHT", "ACKED", "DEADLETTERS", "DROPPED"}
	rows := make([][]string, 0, len(names))
	for _, name := range names {
		st := byTopic[name]
		rows = append(rows, []string{
			name,
			strconv.Itoa(st.Enqueued),
			strconv.Itoa(st.Ready),
			strconv.Itoa(st.InFlight),
			strconv.Itoa(st.Acked),
			strconv.Itoa(st.DeadLetters),
			strconv.Itoa(st.Dropped),
		})
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	var b strings.Builder
	writeRow := func(cells []string) {
		for i, cell := range cells {
			if i > 0 {
				b.WriteString("  ")
			}
			fmt.Fprintf(&b, "%-*s", widths[i], cell)
		}
		b.WriteByte('\n')
	}
	writeRow(headers)
	for _, row := range rows {
		writeRow(row)
	}
	return b.String()
}
