package db

import (
	"testing"

	_ "modernc.org/sqlite" //
)

func TestDB(t *testing.T) {
	d, err := Init("sqlite", "/home/michael/test.db")
	if err != nil {
		t.Fatal(err)
	}

	_ = d

	// fmt.Println(d.CreateSet("setA", "me", "", "some path", 0))
	// fmt.Println(d.CreateSet("setB", "me", "", "some path", 0))
	// fmt.Println(d.CreateSet("setB", "me", "", "some path", 0))
	// fmt.Println(d.CreateSet("setC", "me", "", "some other path", 0))
	// fmt.Println(d.CreateSet("setC", "not me", "", "some other path", 0))
}
