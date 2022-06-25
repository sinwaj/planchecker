package plan

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// Represents a node (anything indented with "->" in the plan)
type Node struct {
	// Location in the file used to build the tree
	Indent int
	Offset int

	// Variables parsed from EXPLAIN
	Operator    string
	Object      string // Name of index or table. Only exists for some nodes
	ObjectType  string // TABLE, INDEX, etc...
	Slice       int64
	StartupCost float64
	TotalCost   float64
	NodeCost    float64
	PrctCost    float64
	Rows        int64
	Width       int64

	// Variables parsed from EXPLAIN ANALYZE
	ActualRows        float64
	AvgRows           float64
	Workers           int64
	MaxRows           float64
	MaxSeg            string
	Scans             int64
	MsFirst           float64
	MsEnd             float64
	MsOffset          float64
	MsNode            float64
	MsPrct            float64
	AvgMem            float64
	MaxMem            float64
	ExecMemLine       float64
	SpillFile         int64
	SpillReuse        int64
	PartSelected      int64
	PartSelectedTotal int64
	PartScanned       int64
	PartScannedTotal  int64
	Filter            string

	// Contains all the text lines below each node
	ExtraInfo []string

	// Populated in BuildTree() to link nodes/plans together
	SubNodes []*Node
	SubPlans []*Plan

	// Populated with any warning for the node
	Warnings []Warning

	// Flag to detect if we are looking at EXPLAIN or EXPLAIN ANALYZE output
	IsAnalyzed bool
}

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

	// Init everything to -1
	n.ActualRows = -1
	n.AvgRows = -1
	n.Workers = -1
	n.MaxRows = -1
	n.MaxSeg = "-"
	n.Scans = -1
	n.MsFirst = -1
	n.MsEnd = -1
	n.MsOffset = -1
	n.AvgMem = -1
	n.MaxMem = -1
	n.ExecMemLine = -1
	n.SpillFile = -1
	n.SpillReuse = -1
	n.PartSelected = -1
	n.PartSelectedTotal = -1
	n.PartScanned = -1
	n.PartScannedTotal = -1
	n.Filter = ""
	n.IsAnalyzed = false

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

func (n *Node) CalculateSubNodeDiff() {
	msChild := 0.0
	costChild := 0.0
	for _, s := range n.SubNodes {
		//logDebugf("\tSUBNODE%s", s.Operator)
		msChild += s.MsEnd
		costChild += s.TotalCost
	}

	for _, s := range n.SubPlans {
		//logDebugf("\tSUBPLANNODE%s", s.TopNode.Operator)
		costChild += s.TopNode.TotalCost
	}

	n.MsNode = n.MsEnd - msChild
	n.NodeCost = n.TotalCost - costChild

	if n.MsNode < 0 {
		n.MsNode = 0
	}

	if n.NodeCost < 0 {
		n.NodeCost = 0
	}
}

func (n *Node) CalculatePercentage(totalCost float64, totalMs float64) {
	n.PrctCost = n.NodeCost / totalCost * 100
	n.MsPrct = n.MsNode / totalMs * 100
}

// Render node for output to console
func (n *Node) Render(indent int) {
	indent += 1
	indentString := strings.Repeat(" ", indent*indentDepth)

	if n.Slice > -1 {
		fmt.Printf("\n%s   // Slice %d\n", indentString, n.Slice)
	}

	fmt.Printf("%s-> %s | startup cost %s | total cost %s | rows %d | width %d\n",
		indentString,
		n.Operator,
		n.StartupCost,
		n.TotalCost,
		n.Rows,
		n.Width)

	// Render ExtraInfo
	for _, e := range n.ExtraInfo[1:] {
		fmt.Printf("%s   %s\n", indentString, strings.Trim(e, " "))
	}

	// Render warnings
	for _, w := range n.Warnings {
		fmt.Printf("\x1b[%dm", warningColor)
		fmt.Printf("%s   WARNING: %s | %s\n", indentString, w.Cause, w.Resolution)
		fmt.Printf("\x1b[%dm", 0)
	}

	// Render sub nodes
	for _, s := range n.SubNodes {
		s.Render(indent)
	}

	// Render sub plans
	for _, s := range n.SubPlans {
		s.Render(indent)
	}
}

// Render plan for output to console
func (p *Plan) Render(indent int) {
	indent += 1
	indentString := strings.Repeat(" ", indent*indentDepth)

	fmt.Printf("%s%s\n", indentString, p.Name)
	p.TopNode.Render(indent)
}

// Render explain for output to console
func (e *Explain) PrintPlan() {

	fmt.Println("Plan:")
	e.Plans[0].TopNode.Render(0)

	if len(e.Warnings) > 0 {
		fmt.Printf("\n")
		for _, w := range e.Warnings {
			fmt.Printf("\x1b[%dm", warningColor)
			fmt.Printf("WARNING: %s | %s\n", w.Cause, w.Resolution)
			fmt.Printf("\x1b[%dm", 0)
		}
	}

	fmt.Printf("\n")

	if len(e.SliceStats) > 0 {
		fmt.Println("Slice statistics:")
		for _, stat := range e.SliceStats {
			fmt.Printf("\t%s\n", stat)
		}
	}

	if e.MemoryUsed > 0 {
		fmt.Println("Statement statistics:")
		fmt.Printf("\tMemory used: %d\n", e.MemoryUsed)
		if e.MemoryWanted > 0 {
			fmt.Printf("\tMemory wanted: %d\n", e.MemoryWanted)
		}
	}

	if len(e.Settings) > 0 {
		fmt.Println("Settings:")
		for _, setting := range e.Settings {
			fmt.Printf("\t%s = %s\n", setting.Name, setting.Value)
		}
	}

	if e.OptimizerStatus != "" {
		fmt.Println("Optimizer status:")
		fmt.Printf("\t%s\n", e.OptimizerStatus)
	}

	if e.Runtime > 0 {
		fmt.Println("Total runtime:")
		fmt.Printf("\t%.0f ms\n", e.Runtime)
	}

}

// Main init function
func (e *Explain) InitPlan(plantext string) error {

	// Split the data in to lines
	e.lines = strings.Split(string(plantext), "\n")

	// Parse lines in to node objects
	err := e.parseLines()
	if err != nil {
		return err
	}

	if len(e.Nodes) == 0 {
		return errors.New("Could not find any nodes in plan")
	}

	// Convert array of nodes to tree structure
	e.BuildTree()

	// Parse all nodes first so they are fully populated
	for _, n := range e.Nodes {
		// Parse ExtraInfo
		err := parseNodeExtraInfo(n)
		if err != nil {
			return err
		}
	}

	// If first node is an INSERT node then it will not have any startup or total cost
	// template1=# explain insert INTO tbl1 select * from tbl1 ;
	//     Insert (slice0; segments: 4)  (rows=13200 width=32)
	//       ->  Seq Scan on tbl1  (cost=0.00..628.00 rows=13200 width=32)
	// So copy stats from the first child to make calculations work as expected
	if e.Nodes[0].TotalCost == 0 {
		if len(e.Nodes) >= 2 {
			e.Nodes[0].TotalCost = e.Nodes[1].TotalCost
			e.Nodes[0].StartupCost = e.Nodes[1].StartupCost
			e.Nodes[0].MsEnd = e.Nodes[1].MsEnd
			e.Nodes[0].MsOffset = e.Nodes[1].MsOffset
			e.Nodes[0].IsAnalyzed = e.Nodes[1].IsAnalyzed
		}
	}

	// Loop again to perform checks
	for _, n := range e.Nodes {
		n.CalculateSubNodeDiff()

		// Pass in Cost + Time of top node as it should be equal to total
		n.CalculatePercentage(e.Nodes[0].TotalCost, e.Nodes[0].MsEnd)

		// Run Node checks
		for _, c := range NODECHECKS {
			c.Exec(n)
		}
	}

	// Run Explain checks
	for _, c := range EXPLAINCHECKS {
		c.Exec(e)
	}

	return nil
}

// Init from stdin (useful for psql -f myquery.sql > planchecker)
// planchecker will handle reading from stdin
func (e *Explain) InitFromStdin(debug bool) error {
	logDebug = debug

	logDebugf("InitFromStdin\n")

	fi, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	if fi.Size() == 0 {
		return errors.New("stdin is empty")
	}

	bytes, _ := ioutil.ReadAll(os.Stdin)
	plantext := string(bytes)

	e.InitPlan(plantext)

	return nil
}

// Init from string
func (e *Explain) InitFromString(plantext string, debug bool) error {
	logDebug = debug

	logDebugf("InitFromString\n")

	err := e.InitPlan(plantext)
	if err != nil {
		return err
	}

	return nil
}

// Init from file
func (e *Explain) InitFromFile(filename string, debug bool) error {
	logDebug = debug

	logDebugf("InitFromFile\n")

	// Check file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return err
	}

	// Read all lines
	filedata, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	plantext := string(filedata)

	err = e.InitPlan(plantext)
	if err != nil {
		return err
	}

	return nil
}
