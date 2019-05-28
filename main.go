package main

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/SebastiaanKlippert/go-foxpro-dbf"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	sourceFile := "/home/frederickrobinson/go/src/github.com/frrad/meckleen/parcel_taxdata/Parcel_TaxData.dbf"
	colNames, colTypes, rows := readDbf(sourceFile)

	lookup := map[byte]string{
		'D': "date",
		'F': "float",
		'N': "numeric",
		'C': "character",
	}

	for i, t := range colTypes {
		fmt.Printf("%s\t(%d=%s)\n", colNames[i], t, lookup[t])
	}

	outPath := "/home/frederickrobinson/outfile.sqlite"
	err := writeSQLite(outPath, colNames, colTypes, rows)
	if err != nil {
		log.Fatalf("problem writing outdb: %v", err)
	}
}

func writeSQLite(outPath string, colNames []string, colTypes []byte, rows <-chan []interface{}) error {
	database, err := sql.Open("sqlite3", outPath)
	if err != nil {
		return err
	}
	log.Println("opened db", outPath)

	_, err = database.Exec("CREATE TABLE IF NOT EXISTS people (id INTEGER PRIMARY KEY, firstname TEXT, lastname TEXT)")
	if err != nil {
		return err
	}

	insertStmt, err := database.Prepare("INSERT INTO people (firstname, lastname) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer func() {
		err := insertStmt.Close()
		if err != nil {
			fmt.Println(err)
		}
	}()
	_, err = insertStmt.Exec("Nic", "Raboy")
	if err != nil {
		return err
	}
	return nil
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
