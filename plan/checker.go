package plan

import (
	"fmt"
	"regexp"
)

type NodeCheck struct {
	Name        string
	Description string
	CreatedAt   string
	Scope       []string
	Exec        func(*Node)
}

type ExplainCheck struct {
	Name        string
	Description string
	CreatedAt   string
	Scope       []string
	Exec        func(*Explain)
}

// Keep all checks in NODECHEKS and EXPLAINCHECKS so that we can
// dynamically create a list of checks to display in webapp

// ------------------------------------------------------------
// Checks relating to each node
// ------------------------------------------------------------
var NODECHECKS = []NodeCheck{
	NodeCheck{
		"checkNodeEstimatedRows",
		"Scan node with estimated rows equal to 1",
		"2016-05-24",
		[]string{"orca", "legacy"},
		func(n *Node) {
			re := regexp.MustCompile(`(Dynamic Table|Table|Parquet table|Bitmap Index|Bitmap Append-Only Row-Oriented|Seq) Scan`)
			if re.MatchString(n.Operator) {
				if n.Rows == 1 {
					warningAction := ""
					// Preformat the string here
					if n.ObjectType == "TABLE" {
						warningAction = fmt.Sprintf("ANALYZE on table")
					} else if n.ObjectType == "INDEX" {
						warningAction = fmt.Sprintf("REINDEX on index")
					}

					// If EXPLAIN ANALYZE output then have to check further
					if n.IsAnalyzed == true {
						if n.ActualRows > 1 || n.AvgRows > 1 {
							n.Warnings = append(n.Warnings, Warning{
								"Actual rows is higher than estimated rows",
								fmt.Sprintf("Need to run %s \"%s\"", warningAction, n.Object)})
						}
						// Else just flag as a potential not analyzed table
					} else {
						n.Warnings = append(n.Warnings, Warning{
							"Estimated rows is 1",
							fmt.Sprintf("May need to run %s \"%s\"", warningAction, n.Object)})
					}
				}
			}
		}},
	NodeCheck{
		"checkNodeNestedLoop",
		"Nested Loops",
		"2016-05-23",
		[]string{"orca", "legacy"},
		func(n *Node) {
			re := regexp.MustCompile(`Nested Loop`)
			if re.MatchString(n.Operator) {
				n.Warnings = append(n.Warnings, Warning{
					"Nested Loop",
					"Review query"})
			}
		}},
	NodeCheck{
		"checkNodeSpilling",
		"Spill files",
		"2016-05-31",
		[]string{"orca", "legacy"},
		func(n *Node) {
			if n.SpillFile >= 1 {
				n.Warnings = append(n.Warnings, Warning{
					fmt.Sprintf("Total %d spilling segments found", n.SpillFile),
					"Review query"})
			}
		}},
	NodeCheck{
		"checkNodeScans",
		"Node looping multiple times",
		"2016-05-31",
		[]string{"orca", "legacy"},
		func(n *Node) {
			if n.Scans > 1 {
				n.Warnings = append(n.Warnings, Warning{
					fmt.Sprintf("This node is executed %d times", n.Scans),
					"Review query"})
			}
		}},
	NodeCheck{
		"checkNodePartitionScans",
		"Number of partition scans greater than 100 or 25%%",
		"2016-05-31",
		[]string{"orca", "legacy"},
		func(n *Node) {
			partitionThreshold := int64(100)
			partitionPrctThreshold := int64(25)

			// Planner
			re := regexp.MustCompile(`Append`)
			if re.MatchString(n.Operator) {
				// Warn if the Append node has more than 100 subnodes
				if int64(len(n.SubNodes)) >= partitionThreshold {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("Detected %d partition scans", len(n.SubNodes)),
						"Check if partitions can be eliminated"})
				}
			}

			// ORCA

			// SELECTED
			re = regexp.MustCompile(`Partition Selector`)
			if re.MatchString(n.Operator) && n.PartSelected > -1 {
				// Warn if selected partitions is great than 100
				if n.PartSelected >= partitionThreshold {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("Detected %d partition scans", n.PartSelected),
						"Check if partitions can be eliminated"})
				}

				// Warn if selected partitons is 0, may be an issue
				if n.PartSelected == 0 {
					n.Warnings = append(n.Warnings, Warning{
						"Zero partitions selected",
						"Review query"})
					// Also warn if greater than 25% of total partitions were selected.
					// I just chose 25% for now... may need to be adjusted to a more reasonable value
				} else if (n.PartSelected * 100 / n.PartSelectedTotal) >= partitionPrctThreshold {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("%d%% (%d out of %d) partitions selected", (n.PartSelected * 100 / n.PartSelectedTotal), n.PartSelected, n.PartSelectedTotal),
						"Check if partitions can be eliminated"})
				}
			}

			// SCANNED
			re = regexp.MustCompile(`Dynamic Table Scan`)
			if re.MatchString(n.Operator) && n.PartScanned > -1 {
				// Warn if scanned partitions is great than 100
				if n.PartScanned >= partitionThreshold {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("Detected %d partition scans", n.PartScanned),
						"Check if partitions can be eliminated"})
				}

				// Warn if scanned partitons is 0, may be an issue
				if n.PartScanned == 0 {
					n.Warnings = append(n.Warnings, Warning{
						"Zero partitions scanned",
						"Review query"})
					// Also warn if greater than 25% of total partitions were scanned.
					// I just chose 25% for now... may need to be adjusted to a more reasonable value
				} else if (n.PartScanned * 100 / n.PartScannedTotal) >= partitionPrctThreshold {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("%d%% (%d out of %d) partitions scanned", (n.PartScanned * 100 / n.PartScannedTotal), n.PartScanned, n.PartScannedTotal),
						"Check if partitions can be eliminated"})
				}
			}
		}},
	NodeCheck{
		"checkNodeDataSkew",
		"Data skew",
		"2016-06-02",
		[]string{"orca", "legacy"},
		func(n *Node) {
			threshold := 10000.0

			// Only proceed if over threshold
			if n.ActualRows >= threshold || n.AvgRows >= threshold {
				// Handle AvgRows
				if n.AvgRows > 0 {
					// A segment has more than 50% of all rows
					// Only do this if workers > 2 otherwise this situation will report skew:
					//     Rows out:  Avg 500000.0 rows x 2 workers.  Max 500001 rows (seg0)
					// but seg0 only has 1 extra row
					if (n.MaxRows > (n.AvgRows * float64(n.Workers) / 2.0)) && n.Workers > 2 {
						n.Warnings = append(n.Warnings, Warning{
							fmt.Sprintf("Data skew on segment %s", n.MaxSeg),
							"Review query"})
					}
					// Handle ActualRows
					// If ActualRows is set and MaxSeg is set then this
					// segment has the highest rows
				} else if n.ActualRows > 0 && n.MaxSeg != "-" {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("Data skew on segment %s", n.MaxSeg),
						"Review query"})
				}
			}
		}},
	NodeCheck{
		"checkNodeFilterWithFunction",
		"Filter clause using function",
		"2016-06-06",
		[]string{"orca", "legacy"},
		// Example:
		//     upper(brief_status::text) = ANY ('{SIGNED,BRIEF,PROPO}'::text[])
		//
		func(n *Node) {
			re := regexp.MustCompile(`\S+\(.*\) `)

			if re.MatchString(n.Filter) {
				n.Warnings = append(n.Warnings, Warning{
					"Filter using function",
					"Check if function can be avoided"})
			}
		}},
}

// ------------------------------------------------------------
// Checks relating to the over all Explain output
// ------------------------------------------------------------
var EXPLAINCHECKS = []ExplainCheck{
	ExplainCheck{
		"checkExplainMotionCount",
		"Number of Broadcast/Redistribute Motion nodes greater than 5",
		"2016-05-23",
		[]string{"orca", "legacy"},
		func(e *Explain) {
			motionCount := 0
			motionCountLimit := 5

			re := regexp.MustCompile(`(Broadcast|Redistribute) Motion`)

			for _, n := range e.Nodes {
				if re.MatchString(n.Operator) {
					motionCount++
				}
			}

			if motionCount >= motionCountLimit {
				e.Warnings = append(e.Warnings, Warning{
					fmt.Sprintf("Found %d Redistribute/Broadcast motions", motionCount),
					"Review query"})
			}
		}},
	ExplainCheck{
		"checkExplainSliceCount",
		"Number of slices greater than 100",
		"2016-05-31",
		[]string{"orca", "legacy"},
		func(e *Explain) {
			sliceCount := 0
			sliceCountLimit := 100

			for _, n := range e.Nodes {
				if n.Slice > -1 {
					sliceCount++
				}
			}

			if sliceCount > sliceCountLimit {
				e.Warnings = append(e.Warnings, Warning{
					fmt.Sprintf("Found %d slices", sliceCount),
					"Review query"})
			}
		}},
	ExplainCheck{
		"checkExplainPlannerFallback",
		"ORCA fallback to legacy query planner",
		"2016-05-31",
		[]string{"orca"},
		func(e *Explain) {
			// Settings:  optimizer=on
			// Optimizer status: legacy query optimizer
			re := regexp.MustCompile(`legacy query optimizer`)

			if re.MatchString(e.OptimizerStatus) {
				for _, s := range e.Settings {
					if s.Name == "optimizer" && s.Value == "on" {
						e.Warnings = append(e.Warnings, Warning{
							"ORCA enabled but plan was produced by legacy query optimizer",
							"No Action Required"})
						break
					}
				}
			}
		}},
	ExplainCheck{
		"checkExplainEnableGucNonDefault",
		"\"enable_\" GUCs configured with non-default values",
		"2016-06-06",
		[]string{"orca", "legacy"},
		func(e *Explain) {
			// Default GUC values.
			// http://gpdb.docs.pivotal.io/4340/guc_config-topic3.html
			defaults := map[string]string{
				"enable_bitmapscan": "on",
				"enable_groupagg":   "on",
				"enable_hashagg":    "on",
				"enable_hashjoin":   "on",
				"enable_indexscan":  "on",
				"enable_seqscan":    "on",
				"enable_sort":       "on",
				"enable_tidscan":    "on",
				"enable_nestloop":   "off",
				"enable_mergejoin":  "off",
			}

			// Settings:  enable_hashjoin=off; enable_indexscan=off; join_collapse_limit=1; optimizer=on
			re := regexp.MustCompile(`enable_`)

			for _, s := range e.Settings {
				if re.MatchString(s.Name) {
					if value, ok := defaults[s.Name]; ok {
						// Only report if NOT default value
						if s.Value != value {
							e.Warnings = append(e.Warnings, Warning{
								fmt.Sprintf("\"%s\" GUC has non-default value \"%s\"", s.Name, s.Value),
								fmt.Sprintf("Check if \"%s\" GUC is required", s.Name)})
						}
					}
				}
			}
		}},
	ExplainCheck{
		"checkExplainOrcaChildPartitionScan",
		"Scan on child partition instead of root partition",
		"2016-06-08",
		[]string{"orca"},
		func(e *Explain) {

			// Skip if using legacy
			if e.Optimizer != "on" {
				return
			}

			// ->  Seq Scan on sales_1_prt_outlying_years s  (cost=0.00..55276.72 rows=2476236 width=8)
			// ->  Seq Scan on sales_1_prt_2 s  (cost=0.00..38.44 rows=1722 width=8)
			re := regexp.MustCompile(`_[0-9]+_prt_`)

			for _, n := range e.Nodes {
				// Check if object name looks like partition
				if re.MatchString(n.Operator) {
					n.Warnings = append(n.Warnings, Warning{
						fmt.Sprintf("Scan on what appears to be a child partition"),
						fmt.Sprintf("Recommend using root partition when ORCA is enabled")})
				}
			}
		}},
}
