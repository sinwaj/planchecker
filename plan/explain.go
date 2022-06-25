package plan

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// GUCs are parsed so can do checks for specific settings
type Setting struct {
	Name  string
	Value string
}

// Top level object
type Explain struct {
	Nodes           []*Node // All nodes get added here
	Plans           []*Plan // All plans get added here
	SliceStats      []string
	MemoryUsed      int64
	MemoryWanted    int64
	Settings        []Setting
	Optimizer       string
	OptimizerStatus string
	Runtime         float64

	// Populated with any warning for the overall EXPLAIN output
	Warnings []Warning

	lines        []string
	lineOffset   int
	planFinished bool
}

// ------------------------------------------------------------
// ->  Seq Scan on sales_1_prt_outlying_years sales  (cost=0.00..67657.90 rows=2477 width=8)
func (e *Explain) createNode(line string) *Node {
	logDebugf("createNode\n")
	// Set node indent
	// Rest of node parsing is handled in parseNodeExtraInfo
	node := new(Node)
	node.Indent = getIndent(line)
	node.Offset = e.lineOffset
	node.ExtraInfo = []string{
		line,
	}

	return node
}

// ------------------------------------------------------------
// SubPlan 2
//   ->  Limit  (cost=0.00..0.64 rows=1 width=0)
//         ->  Seq Scan on pg_attribute c2  (cost=0.00..71.00 rows=112 width=0)
//               Filter: atttypid = $1
//
func (e *Explain) createPlan(line string) *Plan {
	logDebugf("createPlan\n")

	plan := new(Plan)
	plan.Name = strings.Trim(line, " ")
	plan.Indent = getIndent(line)
	plan.Offset = e.lineOffset
	plan.TopNode = new(Node)

	return plan
}

// ------------------------------------------------------------
// Settings:  enable_hashjoin=off; enable_indexscan=off; join_collapse_limit=1; optimizer=on
// Settings:  optimizer=off
//
func (e *Explain) parseSettings(line string) {
	logDebugf("parseSettings\n")
	e.planFinished = true
	line = strings.TrimSpace(line)
	line = line[11:]
	settings := strings.Split(line, "; ")
	for _, setting := range settings {
		temp := strings.Split(setting, "=")
		e.Settings = append(e.Settings, Setting{temp[0], temp[1]})
		logDebugf("\t%s\n", setting)

		// Store actual status of optimizer
		if temp[0] == "optimizer" {
			e.Optimizer = temp[1]
		}
	}
}

// ------------------------------------------------------------
// Slice statistics:
//   (slice0) Executor memory: 2466K bytes.
//   (slice1) Executor memory: 4146K bytes avg x 96 workers, 4146K bytes max (seg7).
//   (slice2) * Executor memory: 153897K bytes avg x 96 workers, 153981K bytes max (seg71). Work_mem: 153588K bytes max, 1524650K bytes wanted.
//
func (e *Explain) parseSliceStats(line string) {
	logDebugf("parseSliceStats\n")
	e.planFinished = true
	for i := e.lineOffset + 1; i < len(e.lines); i++ {
		if getIndent(e.lines[i]) > 1 {
			logDebugf("%s\n", e.lines[i])
			e.SliceStats = append(e.SliceStats, strings.TrimSpace(e.lines[i]))
		} else {
			e.lineOffset = i - 1
			break
		}
	}
}

// ------------------------------------------------------------
// Statement statistics:
//   Memory used: 128000K bytes
//   Memory wanted: 1525449K bytes
//
func (e *Explain) parseStatementStats(line string) {
	logDebugf("parseStatementStats\n")
	e.planFinished = true

	e.MemoryUsed = -1
	e.MemoryWanted = -1

	for i := e.lineOffset + 1; i < len(e.lines); i++ {
		if getIndent(e.lines[i]) > 1 {
			logDebugf(e.lines[i])
			if patterns["STATEMENTSTATS_USED"].MatchString(e.lines[i]) {
				groups := patterns["STATEMENTSTATS_USED"].FindStringSubmatch(e.lines[i])
				e.MemoryUsed, _ = strconv.ParseInt(strings.TrimSpace(groups[1]), 10, 64)
			} else if patterns["STATEMENTSTATS_WANTED"].MatchString(e.lines[i]) {
				groups := patterns["STATEMENTSTATS_WANTED"].FindStringSubmatch(e.lines[i])
				e.MemoryWanted, _ = strconv.ParseInt(strings.TrimSpace(groups[1]), 10, 64)
			}
		} else {
			e.lineOffset = i - 1
			break
		}
	}
}

// ------------------------------------------------------------
//  Optimizer status: legacy query optimizer
//  Optimizer status: PQO version 1.620
//
func (e *Explain) parseOptimizer(line string) {
	logDebugf("PARSE OPTIMIZER\n")
	e.planFinished = true
	line = strings.TrimSpace(line)
	line = line[11:]
	temp := strings.Split(line, ": ")
	e.OptimizerStatus = temp[1]
	logDebugf("\t%s\n", e.OptimizerStatus)
}

// ------------------------------------------------------------
// Total runtime: 7442.441 ms
//
func (e *Explain) parseRuntime(line string) {
	logDebugf("PARSE RUNTIME\n")
	e.planFinished = true
	line = strings.TrimSpace(line)
	temp := strings.Split(line, " ")
	if s, err := strconv.ParseFloat(temp[2], 64); err == nil {
		e.Runtime = s
	}
	logDebugf("\t%f\n", e.Runtime)
}

// Parse all the lines in to empty structs with only ExtraInfo populated
func (e *Explain) parseLines() error {
	logDebugf("ParseLines\n")
	logDebugf("Parsing %d lines\n", len(e.lines))
	e.planFinished = false

	// Check every line for quotes.
	// Easier to do it in one go here rather than multiple places in code.
	for e.lineOffset = 0; e.lineOffset < len(e.lines); e.lineOffset++ {
		e.lines[e.lineOffset] = checkQuote(e.lines[e.lineOffset])
	}

	var err error
	// Loop through lines
	for e.lineOffset = 0; e.lineOffset < len(e.lines); e.lineOffset++ {
		logDebugf("------------------------------ LINE %d ------------------------------\n", e.lineOffset+1)
		logDebugf("%s\n", e.lines[e.lineOffset])
		err = e.parseline(e.lines[e.lineOffset])
		if err != nil {
			return err
		}
	}

	return nil
}

// Parse each line
func (e *Explain) parseline(line string) error {
	// Check if line has doublequotes at start and end i.e. it was copied from pgAdmin output

	indent := getIndent(line)

	// Ignore whitespace, "QUERY PLAN" and "-"
	if len(strings.TrimSpace(line)) == 0 || strings.Index(line, "QUERY PLAN") > -1 || line[:1] == "-" {
		logDebugf("SKIPPING\n")

	} else if patterns["NODE"].MatchString(line) {
		// Parse a new node
		newNode := e.createNode(line)

		if len(e.Nodes) == 0 && newNode.Indent > 1 {
			return errors.New(fmt.Sprintf("Detected wrong indentation on first plan node:\n%s\n\nRecommend running EXPLAIN again and resubmitting the plan.\nDo not manually adjust the indentation as this will lead to incorrect parsing!\n", strings.TrimRight(line, " ")))
		}

		if len(e.Nodes) > 0 && newNode.Indent < 2 {
			return errors.New(fmt.Sprintf("Detected wrong indentation on line:\n%s\n\nRecommend running EXPLAIN again and resubmitting the plan.\nDo not manually adjust the indentation as this will lead to incorrect parsing!\n", strings.TrimRight(line, " ")))
		}

		// If this is the first node then insert the TopPlan also
		if len(e.Nodes) == 0 {
			newPlan := e.createPlan("Plan")
			e.Plans = append(e.Plans, newPlan)
		}

		// Append node to Nodes array
		e.Nodes = append(e.Nodes, newNode)

	} else if patterns["SUBPLAN"].MatchString(line) {
		// Parse a new plan
		newPlan := e.createPlan(line)

		// Append plan to Plans array
		e.Plans = append(e.Plans, newPlan)

	} else if patterns["SLICESTATS"].MatchString(line) {
		e.parseSliceStats(line)

	} else if patterns["STATEMENTSTATS"].MatchString(line) {
		e.parseStatementStats(line)
	} else if patterns["SETTINGS"].MatchString(line) {
		e.parseSettings(line)

	} else if patterns["OPTIMIZER"].MatchString(line) {
		e.parseOptimizer(line)

	} else if patterns["RUNTIME"].MatchString(line) {
		e.parseRuntime(line)

	} else if indent > 1 && e.planFinished == false {
		// Only add if node exists
		if len(e.Nodes) > 0 {
			// Append this line to ExtraInfo on the last node
			e.Nodes[len(e.Nodes)-1].ExtraInfo = append(e.Nodes[len(e.Nodes)-1].ExtraInfo, line)
		}
	} else {
		logDebugf("SKIPPING\n")

	}

	return nil
}

// Populate SubNodes/SubPlans arrays for each node, which results
// in a tree structre with Plans[0] being the top most object:
// Plan 0
//     TopNode
//         SubNodes[]
//             Node 0
//                 SubNodes[]
//                 SubPlans[]
//             Node 1
//                 SubNodes[]
//                 SubPlans[]
//         SubPlans[]
//             Plan 0
//                 TopNode
//                     SubNodes[]
//                         Node 0
//                             SubNodes[]
//                             SubPlans[]
//                     SubPlans[]
//
func (e *Explain) BuildTree() {
	logDebugf("########## START BUILD TREE ##########\n")

	// Walk backwards through the Plans array and a
	logDebugf("########## PLANS ##########\n")
	for i := len(e.Plans) - 1; i > -1; i-- {
		logDebugf("%d %s\n", e.Plans[i].Indent, e.Plans[i].Name)

		// Loop upwards to find parent
		for p := len(e.Nodes) - 1; p > -1; p-- {
			logDebugf("\t%d %s\n", e.Nodes[p].Indent, e.Nodes[p].Operator)
			if e.Plans[i].Indent > e.Nodes[p].Indent && e.Plans[i].Offset > e.Nodes[p].Offset {
				logDebugf("\t\tFOUND PARENT NODE\n")
				// Prepend to start of array to keep ordering
				e.Nodes[p].SubPlans = append([]*Plan{e.Plans[i]}, e.Nodes[p].SubPlans...)
				break
			}
		}
	}

	// Insert Nodes
	logDebugf("########## NODES ##########\n")
	for i := len(e.Nodes) - 1; i > -1; i-- {
		logDebugf("%d %s\n", e.Nodes[i].Indent, e.Nodes[i].Operator)

		foundParent := false

		// Loop upwards to find parent

		// First check for parent plans
		for p := len(e.Plans) - 1; p > -1; p-- {
			logDebugf("\t%d %s\n", e.Plans[p].Indent, e.Plans[p].Name)
			// If the parent is a SubPlan it will always be Indent-2 and Offset-1
			//  SubPlan 1
			//    ->  Limit  (cost=0.00..9.23 rows=1 width=0)
			if (e.Nodes[i].Indent-2) == e.Plans[p].Indent && (e.Nodes[i].Offset-1) == e.Plans[p].Offset {
				logDebugf("\t\tFOUND PARENT PLAN\n")
				// Prepend to start of array to keep ordering
				e.Plans[p].TopNode = e.Nodes[i]
				foundParent = true
				break
			}
		}

		if foundParent == true {
			continue
		}

		foundParent = false

		// Then check for parent nodes
		for p := i - 1; p > -1; p-- {
			logDebugf("\t%d %s\n", e.Nodes[p].Indent, e.Nodes[p].Operator)
			if e.Nodes[i].Indent > e.Nodes[p].Indent {
				logDebugf("\t\tFOUND PARENT NODE\n")
				// Prepend to start of array to keep ordering
				e.Nodes[p].SubNodes = append([]*Node{e.Nodes[i]}, e.Nodes[p].SubNodes...)
				foundParent = true
				break
			}
		}

		//
		if foundParent == false {
			logDebugf("\t\tTOPNODE\n")
			e.Plans[0].TopNode = e.Nodes[i]
		}
	}

	logDebugf("########## END BUILD TREE ##########\n")
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
