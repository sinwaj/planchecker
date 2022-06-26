package plan

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Each plan has a top node
type Plan struct {
	Name    string
	Indent  int
	Offset  int
	TopNode *Node
}

// Warnings get added to the overall Explain object or a Node object
type Warning struct {
	Cause      string // What caused the warning
	Resolution string // What should be done to resolve it
}

// Slice stats parsed from EXPLAIN ANALYZE output
type SliceStat struct {
	Name          string
	MemoryAvg     int64
	Workers       int64
	MemoryMax     int64
	WorkMem       int64
	WorkMemWanted int64
}

var (
	logDebug     bool
	indentDepth  = 4  // Used for printing the plan
	warningColor = 31 // RED

)

func logDebugf(format string, v ...interface{}) {
	if logDebug == true {
		fmt.Printf(format, v...)
	}
}

// Calculate indent by triming white space and checking diff on string length
func getIndent(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}

// Example data to be parsed
//   ->  Hash Join  (cost=0.00..862.00 rows=1 width=16)
//         Hash Cond: public.sales.id = public.sales.year
//         Rows out:  11000 rows (seg0) with 6897 ms to first row, 7429 ms to end, start offset by 40 ms.
//         Executor memory:  127501K bytes avg, 127501K bytes max (seg0).
//         Work_mem used:  127501K bytes avg, 127501K bytes max (seg0). Workfile: (2 spilling, 0 reused)
//         Work_mem wanted: 171875K bytes avg, 171875K bytes max (seg0) to lessen workfile I/O affecting 2 workers.
func parseNodeExtraInfo(n *Node) error {
	// line 0 will always be the node line
	// Example:
	//     ->  Broadcast Motion 1:2  (slice1)  (cost=0.00..27.48 rows=1124 width=208)
	line := n.ExtraInfo[0]

	groups := patterns["NODE"].FindStringSubmatch(line)

	n.Object = ""
	n.ObjectType = ""

	if len(groups) == 7 {
		// Remove the indent arrow
		groups[1] = strings.Trim(groups[1], " ->")

		// Check if the string contains slice information
		sliceGroups := patterns["SLICE"].FindStringSubmatch(groups[1])
		if len(sliceGroups) == 3 {
			n.Operator = strings.TrimSpace(sliceGroups[1])
			n.Slice, _ = strconv.ParseInt(strings.TrimSpace(sliceGroups[2]), 10, 64)
			// Else it's just the operator
		} else {
			n.Operator = strings.TrimSpace(groups[1])
			n.Slice = -1
		}

		// Try to get object name if this is a scan node
		// Look for non index scans
		re := regexp.MustCompile(`(Index ){0,0} Scan (on|using) (\S+)`)
		temp := re.FindStringSubmatch(n.Operator)
		if len(temp) == re.NumSubexp()+1 {
			n.Object = temp[3]
			n.ObjectType = "TABLE"
		}

		// Look for index scans
		re = regexp.MustCompile(`Index.*Scan (on|using) (\S+)`)
		temp = re.FindStringSubmatch(n.Operator)
		if len(temp) == re.NumSubexp()+1 {
			n.Object = temp[2]
			n.ObjectType = "INDEX"
		}

		// Store the remaining params
		n.StartupCost, _ = strconv.ParseFloat(strings.TrimSpace(groups[3]), 64)
		n.TotalCost, _ = strconv.ParseFloat(strings.TrimSpace(groups[4]), 64)
		n.Rows, _ = strconv.ParseInt(strings.TrimSpace(groups[5]), 10, 64)
		n.Width, _ = strconv.ParseInt(strings.TrimSpace(groups[6]), 10, 64)

	} else {
		return errors.New("Unable to parse node")
	}

	n.Init()

	// Parse the remaining lines
	var re *regexp.Regexp
	var m []string

	for _, line := range n.ExtraInfo[1:] {
		logDebugf("%s\n", line)

		// ROWS
		re = regexp.MustCompile(`ms to end`)
		if re.MatchString(line) {
			n.IsAnalyzed = true
			re = regexp.MustCompile(`(\d+) rows at destination`)
			m := re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.ActualRows = s
					logDebugf("ActualRows %f\n", n.ActualRows)
				}
			}

			re = regexp.MustCompile(`(\d+) rows with \S+ ms`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.ActualRows = s
					logDebugf("ActualRows %f\n", n.ActualRows)
				}
			}

			re = regexp.MustCompile(`Max (\S+) rows`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxRows = s
					logDebugf("MaxRows %f\n", n.MaxRows)
				}
			}

			re = regexp.MustCompile(` (\S+) ms to first row`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsFirst = s
					logDebugf("MsFirst %f\n", n.MsFirst)
				}
			}

			re = regexp.MustCompile(` (\S+) ms to end`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsEnd = s
					logDebugf("MsEnd %f\n", n.MsEnd)
				}
			}

			re = regexp.MustCompile(`start offset by (\S+) ms`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MsOffset = s
					logDebugf("MsOffset %f\n", n.MsOffset)
				}
			}

			re = regexp.MustCompile(`Avg (\S+) `)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.AvgRows = s
					logDebugf("AvgRows %f\n", n.AvgRows)
				}
			}

			re = regexp.MustCompile(` x (\d+) workers`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseInt(m[1], 10, 64); err == nil {
					n.Workers = s
					logDebugf("Workers %d\n", n.Workers)
				}
			}

			re = regexp.MustCompile(`of (\d+) scans`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseInt(m[1], 10, 64); err == nil {
					n.Scans = s
					logDebugf("Scans %d\n", n.Scans)
				}
			}

			re = regexp.MustCompile(` \((seg\d+)\) `)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				n.MaxSeg = m[1]
				logDebugf("MaxSeg %s\n", n.MaxSeg)
			}

			re = regexp.MustCompile(`Max (\S+) rows \(`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxRows = s
				}
				logDebugf("MaxRows %f\n", n.MaxRows)

			} else {
				// Only execute this if "Max" was not found
				re = regexp.MustCompile(` (\S+) rows \(`)
				m = re.FindStringSubmatch(line)
				if len(m) == re.NumSubexp()+1 {
					if s, err := strconv.ParseFloat(m[1], 64); err == nil {
						n.ActualRows = s
					}
					logDebugf("ActualRows %f\n", n.ActualRows)
				}
			}
		}

		// MEMORY
		re = regexp.MustCompile(`Work_mem used`)
		if re.MatchString(line) {
			re = regexp.MustCompile(`Work_mem used:\s+(\d+)K bytes avg`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.AvgMem = s
					logDebugf("AvgMem %f\n", n.AvgMem)
				}
			}

			re = regexp.MustCompile(`\s+(\d+)K bytes max`)
			m = re.FindStringSubmatch(line)
			if len(m) == re.NumSubexp()+1 {
				if s, err := strconv.ParseFloat(m[1], 64); err == nil {
					n.MaxMem = s
					logDebugf("MaxMem %f\n", n.MaxMem)
				}
			}
		}

		// SPILL
		re = regexp.MustCompile(`\((\d+) spilling,\s+(\d+) reused\)`)
		m = re.FindStringSubmatch(line)
		if len(m) == re.NumSubexp()+1 {
			n.SpillFile, _ = strconv.ParseInt(strings.TrimSpace(m[1]), 10, 64)
			n.SpillReuse, _ = strconv.ParseInt(strings.TrimSpace(m[2]), 10, 64)
			logDebugf("SpillFile %d\n", n.SpillFile)
			logDebugf("SpillReuse %d\n", n.SpillReuse)
		}

		// PARTITION SELECTED
		re = regexp.MustCompile(`Partitions selected:  (\d+) \(out of (\d+)\)`)
		m = re.FindStringSubmatch(line)
		if len(m) == re.NumSubexp()+1 {
			n.PartSelected, _ = strconv.ParseInt(strings.TrimSpace(m[1]), 10, 64)
			n.PartSelectedTotal, _ = strconv.ParseInt(strings.TrimSpace(m[2]), 10, 64)
			logDebugf("PartSelectedTotal %d\n", n.PartSelectedTotal)
			logDebugf("PartSelected %d\n", n.PartSelected)
		}

		// PARTITION SCANNED
		re = regexp.MustCompile(`Partitions scanned:  (Avg ){0,}(.*) \(out of (\d+)\)`)
		m = re.FindStringSubmatch(line)
		if len(m) > 0 {
			partScannedFloat, _ := strconv.ParseFloat(strings.TrimSpace(m[len(m)-2]), 64)
			n.PartScanned = int64(partScannedFloat)
			n.PartScannedTotal, _ = strconv.ParseInt(strings.TrimSpace(m[len(m)-1]), 10, 64)
			logDebugf("PartScannedTotal %d\n", n.PartScannedTotal)
			logDebugf("PartScanned %d\n", n.PartScanned)
		}

		// FILTER
		re = regexp.MustCompile(`Filter: (.*)`)
		m = re.FindStringSubmatch(line)
		if len(m) == re.NumSubexp()+1 {
			n.Filter = m[1]
			logDebugf("Filter %s\n", n.Filter)
		}

		// #Executor memory:  4978K bytes avg, 39416K bytes max (seg2).
		// if ( $info_line =~ m/Executor memory:/ ) {
		//     $exec_mem_line .= $info_line."\n";
		// }

	}

	// From Greenplum code
	//     Show elapsed time just once if they are the same or if we don't have
	//     any valid elapsed time for first tuple.
	// So set it here to avoid having to handle it later
	if n.MsFirst == -1 {
		n.MsFirst = n.MsEnd
	}

	return nil
}

// Check for quotes
func checkQuote(line string) string {
	if len(line) > 2 {
		if `"` == line[0:1] && `"` == line[len(line)-2:len(line)-1] {
			// If so then remove the doublequotes and add an extra space
			// The space is so the output matches standard psql output
			line = " " + line[1:len(line)-2]
		}
	}
	return line
}

// Render plan for output to console
func (p *Plan) Render(indent int) {
	indent += 1
	indentString := strings.Repeat(" ", indent*indentDepth)

	fmt.Printf("%s%s\n", indentString, p.Name)
	p.TopNode.Render(indent)
}
