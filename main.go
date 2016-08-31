package main

import (
	"database/sql"
	"flag"
	"log"

	_ "github.com/chrislusf/glow/driver"
	"github.com/chrislusf/glow/flow"
	_ "github.com/lib/pq"
)

/*
type Log struct {
	InfoHash string
	Country  string
}

func Source(f *flow.FlowContext, cdbURL string, shard int) *flow.Dataset {
	locations, err := List(cdbURL) // XXX returns only one element for now
	if err != nil {
		log.Fatalf("Can not list files under %s:%v", cdbURL, err)
	}

	return f.Slice(locations).Partition(shard)
}

// List generates a full list of file locations under the given
// location, which should have a prefix of hdfs://
func List(cdbURL string) (locations []string, err error) {
	return []string{cdbURL}, nil
}
*/
func iterate(dbURL string, fn func(rows *sql.Rows)) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Printf("error connection to the database: %s", err)
		return
	}
	selectQuery := `select rowid, infohash, country, address from get_peers_log`
	rows, err := db.Query(selectQuery) // select infohash from get_peers_log
	if err != nil {
		log.Println(err) // Any way to give feedback about errors?
		return
	}
	fn(rows)
	rows.Close()

}

const shards = 3

type result struct {
	rowid    string
	infohash string
	country  sql.NullString
	address  string
}

var (
	f = flow.New()
)

func init() {
	f.Source(func(ch chan result) {
		iterate("postgresql://maxroach@localhost:26257/roachy?sslmode=disable", func(rows *sql.Rows) {
			for rows.Next() {
				var r result
				if err := rows.Scan(&r.rowid, &r.infohash, &r.country, &r.address); err != nil {
					log.Println(err)
				} else {
					ch <- r
				}
			}
		})
	}, shards).Filter(func(row result) bool {
		// find lines with empty Country
		return !row.country.Valid // && row.country.String == ""
	}).Map(func(row result) string {
		return row.country.String
	}).Map(func(key string) int {
		return 1
	}).Reduce(func(x int, y int) int {
		return x + y
	}).Map(func(x int) {
		println("count:", x)
	})

}

func main() {
	flag.Parse()
	flow.Ready()
	f.Run()
}
