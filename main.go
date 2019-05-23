package main

import (
	"fmt"

	"github.com/SebastiaanKlippert/go-foxpro-dbf"
)

func main() {
	colNames, colTypes, rows := readDbf("/home/frederickrobinson/go/src/github.com/frrad/meckleen/parcel_taxdata/Parcel_TaxData.dbf")

	lookup := map[byte]string{
		'D': "date",
		'F': "float",
		'N': "numeric",
		'C': "character",
	}

	for i, t := range colTypes {
		fmt.Printf("%s(%d=%s) ", colNames[i], t, lookup[t])
	}

	for i := 0; i < 1; i++ {
		fmt.Println(<-rows)
	}

	writeSQLite(colNames, colTypes, rows)
}

func writeSQLite(colNames []string, colTypes []byte, rows <-chan []interface{}) {

}

func columnTypes(headers []dbf.FieldHeader) []byte {
	ans := []byte{}
	for _, x := range headers {
		ans = append(ans, x.Type)
	}

	return ans
}

func readDbf(filename string) ([]string, []byte, <-chan []interface{}) {
	hax := func(version byte) error {
		if version == 0x03 {
			return nil
		}
		return fmt.Errorf("butts")
	}

	dbf.ValidFileVersionFunc = hax
	testdbf, err := dbf.OpenFile(filename, new(dbf.Win1250Decoder))

	if err != nil {
		panic(err)
	}

	headerTypes := columnTypes(testdbf.Fields())
	headerNames := testdbf.FieldNames()

	rowChan := make(chan []interface{}, 1<<10)

	go func(x *dbf.DBF, output chan<- []interface{}) {
		defer testdbf.Close()
		defer close(output)

		max := x.NumRecords()
		for i := uint32(0); i < max; i++ {
			r, err := x.RecordAt(i)
			if err != nil {
				continue
			}
			if !r.Deleted {
				output <- r.FieldSlice()
			}
		}

	}(testdbf, rowChan)

	return headerNames, headerTypes, rowChan
}
