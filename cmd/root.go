package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/SebastiaanKlippert/go-foxpro-dbf"
	_ "github.com/mattn/go-sqlite3"
	"github.com/spf13/cobra"
	"gopkg.in/cheggaaa/pb.v1"
)

var (
	infile    string
	outfile   string
	tablename string

	stripstrings bool
)

func init() {
	rootCmd.Flags().StringVar(&infile, "infile", "", "name of input dbf file")
	rootCmd.MarkFlagRequired("infile")
	rootCmd.Flags().StringVar(&outfile, "outfile", "", "name of output sqlite file")
	rootCmd.MarkFlagRequired("outfile")
	rootCmd.Flags().StringVar(&tablename, "tablename", "", "name of table to add to  sqlite file")
	rootCmd.MarkFlagRequired("tablename")

	rootCmd.Flags().BoolVar(&stripstrings, "stripstrings", false, "should we strip strings while converting")
}

var rootCmd = &cobra.Command{
	Use:   "dbfdump",
	Short: "dbfdump converst dbf to sqlite",
	Long:  `A dbf conversion tool `,
	Run:   convert,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func convert(cmd *cobra.Command, args []string) {
	tableName := tablename
	sourceFile := infile
	outPath := outfile

	colNames, colTypes, rows := readDbf(sourceFile, 0)
	if stripstrings {
		rows = stripRows(rows)
	}

	sqliteTypes, err := sqliteColumnsFromDBF(colTypes)
	if err != nil {
		log.Fatalf("problem translating sqlite column types: %v", err)
	}

	err = writeSQLite(tableName, outPath, colNames, sqliteTypes, rows)
	if err != nil {
		log.Fatalf("problem writing outdb: %v", err)
	}
}

func stripRows(toStrip <-chan []interface{}) <-chan []interface{} {
	out := make(chan []interface{}, 10)

	go func(input <-chan []interface{}, output chan<- []interface{}) {
		for row := range input {
			for i, val := range row {
				strVal, ok := val.(string)
				if !ok {
					continue
				}
				row[i] = strings.Trim(strVal, " ")
			}
			output <- row
		}
		close(output)
	}(toStrip, out)

	return out

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

func buildInsert(tableName string, colNames []string) (string, error) {
	n := len(colNames)
	nameStr, placeholderStr := "", ""

	for i := 0; i < n-1; i++ {
		nameStr += fmt.Sprintf("%s, ", colNames[i])
		placeholderStr += "?, "
	}
	nameStr += fmt.Sprintf("%s", colNames[n-1])
	placeholderStr += "?"

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, nameStr, placeholderStr), nil
}

func buildCreate(tableName string, colNames, colTypes []string) (string, error) {
	n := len(colNames)
	if len(colTypes) != n {
		return "", fmt.Errorf("found %d column names but %d types", len(colNames), len(colTypes))
	}

	ans := ""
	for i := 0; i < n-1; i++ {
		ans += fmt.Sprintf("%s %s, ", colNames[i], colTypes[i])
	}
	ans += fmt.Sprintf("%s %s", colNames[n-1], colTypes[n-1])

	return fmt.Sprintf("CREATE TABLE %s (%s)", tableName, ans), nil
}

func writeSQLite(tableName, outPath string, colNames []string, colTypes []string, rows <-chan []interface{}) error {
	database, err := sql.Open("sqlite3", outPath)
	if err != nil {
		return err
	}
	log.Println("opened db", outPath)

	createString, err := buildCreate(tableName, colNames, colTypes)
	if err != nil {
		return err
	}
	_, err = database.Exec(createString)
	if err != nil {
		return err
	}

	insertString, err := buildInsert(tableName, colNames)
	if err != nil {
		return err
	}
	insertStmt, err := database.Prepare(insertString)
	if err != nil {
		return fmt.Errorf("trouble with %s: %+v", insertString, err)
	}
	defer func() {
		err := insertStmt.Close()
		if err != nil {
			log.Println(err)
		}
	}()

	_, err = database.Exec("BEGIN TRANSACTION")
	for row := range rows {
		_, err = insertStmt.Exec(row...)
		if err != nil {
			return err
		}
	}
	_, err = database.Exec("END TRANSACTION")

	return nil
}

func columnTypes(headers []dbf.FieldHeader) []byte {
	ans := []byte{}
	for _, x := range headers {
		ans = append(ans, x.Type)
	}

	return ans
}

func readDbf(filename string, maxOver uint32) ([]string, []byte, <-chan []interface{}) {
	hax := func(version byte) error {
		if version == 0x03 || version == 0x74 {
			return nil
		}
		return fmt.Errorf("found unexpected version byte %x", version)
	}

	dbf.ValidFileVersionFunc = hax
	testdbf, err := dbf.OpenFile(filename, new(dbf.Win1250Decoder))

	if err != nil {
		panic(err)
	}

	headerTypes := columnTypes(testdbf.Fields())
	headerNames := testdbf.FieldNames()

	rowChan := make(chan []interface{}, 10)

	go func(x *dbf.DBF, output chan<- []interface{}) {
		defer testdbf.Close()
		defer close(output)

		max := x.NumRecords()
		if maxOver > 0 {
			max = maxOver
		}

		bar := pb.StartNew(int(max))

		for i := uint32(0); i < max; i++ {
			bar.Increment()

			r, err := x.RecordAt(i)
			if err != nil {
				continue
			}
			if !r.Deleted {
				output <- r.FieldSlice()
			}
		}
		bar.Finish()

	}(testdbf, rowChan)

	return headerNames, headerTypes, rowChan
}
