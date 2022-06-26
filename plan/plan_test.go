package plan

import (
	"testing"
)

func TestNode_parseExtraInfo(t *testing.T) {
	input := "Gather Motion 2:1  (slice1; segments: 2)  (cost=0.00..431.00 rows=1 width=8)"

	explain := Explain{}
	node := explain.createNode(input)
	response := parseNodeExtraInfo(node)

	if response != nil {
		t.Fatal("It's not a node line")
	}
}
