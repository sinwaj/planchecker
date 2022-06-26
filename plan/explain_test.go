package plan

import "testing"

func TestType1(t *testing.T) {
	explain := Explain{}
	err := explain.InitFromFile("../testdata/explain01.txt", true)

	if err != nil {
		t.Fatal("Type1 test fail")
	}
}
