package plan

import (
	"fmt"
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

// Init everything to -1
func (n *Node) Init() {
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

	fmt.Printf("%s-> %s | startup cost %f | total cost %f | rows %d | width %d\n",
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
