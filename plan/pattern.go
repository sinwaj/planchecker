package plan

import "regexp"

var patterns = map[string]*regexp.Regexp{
	"NODE":    regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`),
	"SLICE":   regexp.MustCompile(`(.*)  \(slice([0-9]*)`),
	"SUBPLAN": regexp.MustCompile(` SubPlan `),

	"SLICESTATS":   regexp.MustCompile(` Slice statistics:`),
	"SLICESTATS_1": regexp.MustCompile(`\((slice[0-9]{1,})\).*Executor memory: ([0-9]{1,})K bytes`),
	"SLICESTATS_2": regexp.MustCompile(`avg x ([0-9]+) workers, ([0-9]+)K bytes max \((seg[0-9]+)\)\.`),
	"SLICESTATS_3": regexp.MustCompile(`Work_mem: ([0-9]+)K bytes max.`),
	"SLICESTATS_4": regexp.MustCompile(`([0-9]+)K bytes wanted.`),

	"STATEMENTSTATS":        regexp.MustCompile(` Statement statistics:`),
	"STATEMENTSTATS_USED":   regexp.MustCompile(`Memory used: ([0-9.-]{1,})K bytes`),
	"STATEMENTSTATS_WANTED": regexp.MustCompile(`Memory wanted: ([0-9.-]{1,})K bytes`),

	"SETTINGS":  regexp.MustCompile(` Settings: `),
	"OPTIMIZER": regexp.MustCompile(` Optimizer status: `),
	"RUNTIME":   regexp.MustCompile(` Total runtime: `),
}
