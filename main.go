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

	sqliteTypes, err := sqliteColumnsFromDBF(colTypes)
	if err != nil {
		log.Fatalf("problem translating sqlite column types: %v", err)
	}

	outPath := "/home/frederickrobinson/outfile.sqlite"
	err = writeSQLite(outPath, colNames, sqliteTypes, rows)
	if err != nil {
		log.Fatalf("problem writing outdb: %v", err)
	}
}

func sqliteColumnsFromDBF(colTypes []byte) ([]string, error) {
	lookup := map[byte]string{
		'D': "DATE",
		'F': "FLOAT",
		'N': "NUMERIC",
		'C': "CHARACTER",
	}

	out := make([]string, len(colTypes))
	for i, inByte := range colTypes {
		name, ok := lookup[inByte]
		if !ok {
			return out, fmt.Errorf("don't know how to decode byte %d", inByte)
		}
		out[i] = name
	}

	return out, nil
}

func buildCreate(colNames, colTypes []string) (string, error) {
	n := len(colNames)
	if len(colTypes) != n {
		return "", fmt.Errorf("found %d column names but %d types", len(colNames), len(colTypes))
	}

	ans := ""
	for i := 0; i < n-1; i++ {
		ans += fmt.Sprintf("%s %s, ", colNames[i], colTypes[i])
	}
	ans += fmt.Sprintf("%s %s", colNames[n-1], colTypes[n-1])

	return fmt.Sprintf("CREATE TABLE people (%s)", ans), nil
}

func writeSQLite(outPath string, colNames []string, colTypes []string, rows <-chan []interface{}) error {
	database, err := sql.Open("sqlite3", outPath)
	if err != nil {
		return err
	}
	log.Println("opened db", outPath)

	createString, err := buildCreate(colNames, colTypes)
	if err != nil {
		return err
	}
	log.Printf("creating table with statement '%s'\n", createString)
	_, err = database.Exec(createString)
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
