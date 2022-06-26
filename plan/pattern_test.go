package plan

import (
	"regexp"
	"testing"
)

func TestNode_full_matched(t *testing.T) {
	input := "Gather Motion 2:1  (slice1; segments: 2)  (cost=0.00..431.00 rows=1 width=8)"
	re := regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`)
	matched := re.MatchString(input)

	if !matched {
		t.Fatal("It's not a node line")
	}
}

func TestNode_full_sub(t *testing.T) {
	input := "Gather Motion 2:1  (slice1; segments: 2)  (cost=0.00..431.00 rows=1 width=8)"
	re := regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`)
	groups := re.FindStringSubmatch(input)

	if groups == nil {
		t.Fatal("parse node line failed")
	}
}

func TestNode_part1_ok(t *testing.T) {
	input := "Gather Motion 2:1   (cost=0.00..431.00 rows=1 width=8)"
	re := regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`)
	matched := re.MatchString(input)

	if !matched {
		t.Fatal("It's not a node line")
	}
}

func TestNode_part2_ok(t *testing.T) {
	input := "Gather Motion 2:1   (rows=1 width=8)"
	re := regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`)
	matched := re.MatchString(input)

	if !matched {
		t.Fatal("It's not a node line")
	}
}

func TestNode_part1_nok(t *testing.T) {
	input := "Gather Motion 2:1   (costx=0.00..431.00 rows=1 width=8)"
	re := regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`)
	matched := re.MatchString(input)

	if matched {
		t.Fatal("It's not a node line")
	}
}

func TestNode_part2_nok(t *testing.T) {
	input := "Gather Motion 2:1   (cost=0.00..431.00 rows1=1 width=8)"
	re := regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`)
	matched := re.MatchString(input)

	if matched {
		t.Fatal("It's not a node line")
	}
}

func TestNode_part3_nok(t *testing.T) {
	input := "Gather Motion 2:1   (cost=0.00..431.00 rows=1 width=8"
	re := regexp.MustCompile(`(.*) \((cost=(.*)\.\.(.*) ){0,1}rows=(.*) width=(.*)\)`)
	matched := re.MatchString(input)

	if matched {
		t.Fatal("It's not a node line")
	}
}
